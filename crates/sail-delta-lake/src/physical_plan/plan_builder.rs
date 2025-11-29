// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::physical_expr::expressions::{Column, NotExpr};
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaFindFilesExec,
    DeltaRemoveActionsExec, DeltaScanByAddsExec, DeltaWriterExec,
};
use crate::datasource::DataFusionMixins;
use crate::options::TableDeltaOptions;
use crate::storage::{default_logstore, StorageConfig};
use crate::table::open_table_with_object_store;

/// Configuration for Delta table operations
pub struct DeltaTableConfig {
    pub table_url: Url,
    pub options: TableDeltaOptions,
    pub partition_columns: Vec<String>,
    pub table_schema_for_cond: Option<SchemaRef>,
    pub table_exists: bool,
}

/// Builder for creating a Delta Lake execution plan with the specified structure:
/// Input -> Project -> Repartition -> Sort -> CoalescePartitions -> Writer -> Commit
/// For OverwriteIf mode: NewData + OldData -> Union -> Writer -> Commit
pub struct DeltaPlanBuilder<'a> {
    input: Arc<dyn ExecutionPlan>,
    table_config: DeltaTableConfig,
    sink_mode: PhysicalSinkMode,
    sort_order: Option<LexRequirement>,
    session_state: &'a dyn Session,
}

impl<'a> DeltaPlanBuilder<'a> {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_config: DeltaTableConfig,
        sink_mode: PhysicalSinkMode,
        sort_order: Option<LexRequirement>,
        session_state: &'a dyn Session,
    ) -> Self {
        Self {
            input,
            table_config,
            sink_mode,
            sort_order,
            session_state,
        }
    }

    /// Build the complete execution plan chain
    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let current_plan = match self.sink_mode.clone() {
            PhysicalSinkMode::OverwriteIf { condition } => {
                self.build_overwrite_if_plan(condition).await?
            }
            _ => self.build_standard_plan()?,
        };

        Ok(current_plan)
    }

    /// Build the standard execution plan for non-OverwriteIf modes
    fn build_standard_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.add_projection_node(self.input.clone())
            .and_then(|plan| self.add_repartition_node(plan))
            .and_then(|plan| self.add_sort_node(plan))
            .and_then(|plan| self.add_writer_node(plan))
            .and_then(|plan| self.add_commit_node(plan))
    }

    /// Build execution plan for OverwriteIf mode
    async fn build_overwrite_if_plan(
        self,
        condition: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Load table to get current version
        let object_store = self
            .session_state
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_config.table_url)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        let storage_config = StorageConfig;
        let log_store = default_logstore(
            object_store.clone(),
            object_store,
            &self.table_config.table_url,
            &storage_config,
        );

        let mut table = crate::table::DeltaTable::new(log_store, Default::default());
        table
            .load()
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        let snapshot_state = table.snapshot()?;
        let version = snapshot_state.version();
        let table_schema = snapshot_state
            .snapshot()
            .arrow_schema()
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Branch 1: Generate Add Actions (new data + preserved old data)
        let old_data_plan = self
            .build_old_data_plan(condition.clone(), version, table_schema.clone())
            .await?;
        let new_data_plan = self
            .add_projection_node(self.input.clone())
            .and_then(|plan| self.add_repartition_node(plan))
            .and_then(|plan| self.add_sort_node(plan))?;

        let (aligned_new_data, aligned_old_data) =
            self.align_schemas(new_data_plan, old_data_plan)?;
        let union_data_plan = UnionExec::try_new(vec![aligned_new_data, aligned_old_data])?;
        let writer_plan = self.add_writer_node(union_data_plan)?;

        // Branch 2: Generate Remove Actions (files to be deleted)
        let find_files_plan = Arc::new(DeltaFindFilesExec::new(
            self.table_config.table_url.clone(),
            Some(condition),
            self.table_config.table_schema_for_cond.clone(),
            version,
        ));
        let remove_actions_plan = Arc::new(DeltaRemoveActionsExec::new(find_files_plan));

        // Merge Action streams
        let union_actions_plan = UnionExec::try_new(vec![writer_plan, remove_actions_plan])?;

        // Commit
        self.add_commit_node(union_actions_plan)
    }

    /// Build the plan for scanning and filtering old data
    async fn build_old_data_plan(
        &self,
        condition: Arc<dyn PhysicalExpr>,
        version: i64,
        table_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Find files that might contain data matching the condition
        let find_files_exec = Arc::new(DeltaFindFilesExec::new(
            self.table_config.table_url.clone(),
            Some(condition.clone()),
            Some(table_schema.clone()),
            version,
        ));

        // Scan the candidate files
        let scan_exec = Arc::new(DeltaScanByAddsExec::new(
            find_files_exec,
            self.table_config.table_url.clone(),
            table_schema,
        ));

        // Filter to keep only data that does NOT match the condition (data to preserve)
        let negated_condition = Arc::new(NotExpr::new(condition));
        let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

        Ok(filter_exec)
    }

    /// Align schemas between new data and old data for UnionExec
    fn align_schemas(
        &self,
        new_data_plan: Arc<dyn ExecutionPlan>,
        old_data_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
        let new_schema = new_data_plan.schema();
        let old_schema = old_data_plan.schema();

        // Ensure both schemas have the same fields
        // TODO: handle schema evolution?
        if new_schema.fields().len() != old_schema.fields().len() {
            return Err(datafusion_common::DataFusionError::Plan(
                "Schema mismatch between new and old data - schema evolution not yet implemented"
                    .to_string(),
            ));
        }

        // Create projection expressions to align field order and types
        let mut new_projections = Vec::new();
        let mut old_projections = Vec::new();

        // Use new schema as the target schema
        for (i, field) in new_schema.fields().iter().enumerate() {
            // For new data, direct mapping
            new_projections.push((
                Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));

            // For old data, find matching field
            if let Some((old_idx, _)) = old_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, old_field)| old_field.name() == field.name())
            {
                old_projections.push((
                    Arc::new(Column::new(field.name(), old_idx)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                ));
            } else {
                return Err(datafusion_common::DataFusionError::Plan(format!(
                    "Field '{}' not found in old data schema",
                    field.name()
                )));
            }
        }

        let aligned_new_data = Arc::new(ProjectionExec::try_new(new_projections, new_data_plan)?);
        let aligned_old_data = Arc::new(ProjectionExec::try_new(old_projections, old_data_plan)?);

        Ok((aligned_new_data, aligned_old_data))
    }

    fn add_projection_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(create_projection(
            input,
            self.table_config.partition_columns.clone(),
        )?)
    }

    fn add_repartition_node(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(create_repartition(
            input,
            self.table_config.partition_columns.clone(),
        )?)
    }

    fn add_sort_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // create_sort handles both partition columns and user-specified sort order
        Ok(create_sort(
            input,
            self.table_config.partition_columns.clone(),
            self.sort_order.clone(),
        )?)
    }

    fn add_writer_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();

        // Extract condition from sink_mode
        let condition = match &self.sink_mode {
            PhysicalSinkMode::OverwriteIf { condition } => Some(condition.clone()),
            _ => None,
        };

        Ok(Arc::new(DeltaWriterExec::new(
            input,
            self.table_config.table_url.clone(),
            self.table_config.options.clone(),
            self.table_config.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_config.table_exists,
            schema,
            condition,
        )))
    }

    fn add_commit_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeltaCommitExec::new(
            input,
            self.table_config.table_url.clone(),
            self.table_config.partition_columns.clone(),
            self.table_config.table_exists,
            self.input.schema(), // Use original input schema for metadata
            self.sink_mode.clone(),
        )))
    }
}

/// Builder for creating a Delta Lake DELETE execution plan.
/// DeltaFindFilesExec -> (Branch A: Rewrite + Branch B: Remove) -> UnionExec -> DeltaCommitExec
/// Branch A: DeltaScanByAddsExec -> FilterExec -> DeltaWriterExec
/// Branch B: DeltaRemoveActionsExec
pub struct DeltaDeletePlanBuilder<'a> {
    table_url: Url,
    condition: Arc<dyn PhysicalExpr>,
    session: &'a dyn Session,
    options: TableDeltaOptions,
}

impl<'a> DeltaDeletePlanBuilder<'a> {
    pub fn new(
        table_url: Url,
        condition: Arc<dyn PhysicalExpr>,
        session: &'a dyn Session,
        options: TableDeltaOptions,
    ) -> Self {
        Self {
            table_url,
            condition,
            session,
            options,
        }
    }

    /// Build the execution plan chain for the DELETE operation.
    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store = self
            .session
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table =
            open_table_with_object_store(self.table_url.clone(), object_store, Default::default())
                .await
                .map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Cannot delete from non-existent Delta table at path: {}. Error: {}",
                        self.table_url, e
                    ))
                })?;

        let snapshot_state = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let version = snapshot_state.version();

        let table_schema = snapshot_state
            .snapshot()
            .arrow_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition_columns = snapshot_state.metadata().partition_columns().clone();

        // Find candidate files
        let find_files_exec = Arc::new(DeltaFindFilesExec::new(
            self.table_url.clone(),
            Some(self.condition.clone()),
            Some(table_schema.clone()),
            version,
        ));

        // --- Branch A: File Rewrite ---

        // Scan the data from candidate files
        let scan_exec = Arc::new(DeltaScanByAddsExec::new(
            find_files_exec.clone(),
            self.table_url.clone(),
            table_schema.clone(),
        ));

        // Filter out rows to be deleted (keep the ones that DON'T match)
        let negated_condition = Arc::new(NotExpr::new(self.condition.clone()));
        let filter_exec = Arc::new(FilterExec::try_new(negated_condition, scan_exec)?);

        // Write the kept rows to new files
        let writer_exec = Arc::new(DeltaWriterExec::new(
            filter_exec,
            self.table_url.clone(),
            self.options.clone(),
            partition_columns.clone(),
            PhysicalSinkMode::Append,
            true, // Table exists
            table_schema.clone(),
            None,
        ));

        // --- Branch B: Generate Remove Actions ---

        // Convert Add actions for old files to Remove actions
        let remove_exec = Arc::new(DeltaRemoveActionsExec::new(find_files_exec));

        // --- Merge and Commit ---

        // Merge the new Add actions and the old Remove actions
        let union_exec = UnionExec::try_new(vec![writer_exec, remove_exec])?;

        // Commit the transaction
        let commit_exec = Arc::new(DeltaCommitExec::new(
            union_exec,
            self.table_url.clone(),
            partition_columns,
            true, // table_exists is true
            table_schema,
            PhysicalSinkMode::Append, // Mode for commit operation is not critical here
        ));

        Ok(commit_exec)
    }
}
