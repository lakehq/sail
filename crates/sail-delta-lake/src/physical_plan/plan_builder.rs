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

use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, NullEquality};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, InListExpr, IsNotNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::utils::split_conjunction;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, Result, ScalarValue};
use futures::TryStreamExt;
use sail_common_datafusion::datasource::{MergeInfo as PhysicalMergeInfo, PhysicalSinkMode};
use url::Url;

use super::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaFindFilesExec,
    DeltaRemoveActionsExec, DeltaScanByAddsExec, DeltaWriterExec,
};
use crate::datasource::{
    DataFusionMixins, DeltaScanConfigBuilder, DeltaTableProvider, PATH_COLUMN,
};
use crate::kernel::models::Add;
use crate::options::TableDeltaOptions;
use crate::storage::{default_logstore, LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store, DeltaTableState};

use std::collections::{HashMap, HashSet};

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

/// Builder for MERGE INTO execution plans.
pub struct DeltaMergePlanBuilder<'a> {
    table_url: Url,
    merge_info: PhysicalMergeInfo,
    session: &'a dyn Session,
    options: TableDeltaOptions,
}

const SOURCE_PRESENT_COLUMN: &str = "__delta_rs_source_present";

#[derive(Debug, Clone)]
struct JoinConditionAnalysis {
    join_keys: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    residual_filter: Option<Arc<dyn PhysicalExpr>>,
    target_only_filters: Vec<Arc<dyn PhysicalExpr>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprColumnDomain {
    None,
    TargetOnly,
    SourceOnly,
    Mixed,
}

impl<'a> DeltaMergePlanBuilder<'a> {
    pub fn new(
        table_url: Url,
        merge_info: PhysicalMergeInfo,
        session: &'a dyn Session,
        options: TableDeltaOptions,
    ) -> Self {
        Self {
            table_url,
            merge_info,
            session,
            options,
        }
    }

    fn analyze_join_condition(&self) -> Result<JoinConditionAnalysis> {
        let num_target = self.merge_info.target_schema.fields().len();
        let num_source = self.merge_info.source_schema.fields().len();
        let mut join_keys: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = Vec::new();
        let mut residual_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut target_only_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();

        for predicate in split_conjunction(&self.merge_info.on_condition.clone()) {
            if let Some((left, right)) =
                self.extract_equality_join_key(&predicate, num_target, num_source)?
            {
                join_keys.push((left, right));
                continue;
            }

            match Self::classify_expr_domain(predicate, num_target, num_source) {
                ExprColumnDomain::TargetOnly => {
                    target_only_filters.push(Arc::clone(predicate));
                    residual_filters.push(Arc::clone(predicate));
                }
                ExprColumnDomain::SourceOnly | ExprColumnDomain::None | ExprColumnDomain::Mixed => {
                    residual_filters.push(Arc::clone(predicate));
                }
            }
        }

        Ok(JoinConditionAnalysis {
            residual_filter: Self::combine_conjunction(&residual_filters),
            join_keys,
            target_only_filters,
        })
    }

    fn extract_equality_join_key(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
        num_target: usize,
        num_source: usize,
    ) -> Result<Option<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> {
        let Some(binary) = predicate.as_any().downcast_ref::<BinaryExpr>() else {
            return Ok(None);
        };

        if binary.op() != &Operator::Eq {
            return Ok(None);
        }

        let left_domain = Self::classify_expr_domain(binary.left(), num_target, num_source);
        let right_domain = Self::classify_expr_domain(binary.right(), num_target, num_source);

        match (left_domain, right_domain) {
            (ExprColumnDomain::TargetOnly, ExprColumnDomain::SourceOnly) => {
                let left = Self::remap_expr_for_side(
                    binary.left(),
                    0,
                    num_target,
                    num_target + num_source,
                )?;
                let right = Self::remap_expr_for_side(
                    binary.right(),
                    num_target,
                    num_source,
                    num_target + num_source,
                )?;
                Ok(Some((left, right)))
            }
            (ExprColumnDomain::SourceOnly, ExprColumnDomain::TargetOnly) => {
                let right = Self::remap_expr_for_side(
                    binary.right(),
                    0,
                    num_target,
                    num_target + num_source,
                )?;
                let left = Self::remap_expr_for_side(
                    binary.left(),
                    num_target,
                    num_source,
                    num_target + num_source,
                )?;
                Ok(Some((right, left)))
            }
            _ => Ok(None),
        }
    }

    fn classify_expr_domain(
        expr: &Arc<dyn PhysicalExpr>,
        num_target: usize,
        _num_source: usize,
    ) -> ExprColumnDomain {
        let mut seen_target = false;
        let mut seen_source = false;

        let mut stack: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(expr)];
        while let Some(node) = stack.pop() {
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                if col.index() < num_target {
                    seen_target = true;
                } else {
                    seen_source = true;
                }
            }

            for child in node.children() {
                stack.push(Arc::clone(child));
            }
        }

        match (seen_target, seen_source) {
            (false, false) => ExprColumnDomain::None,
            (true, false) => ExprColumnDomain::TargetOnly,
            (false, true) => ExprColumnDomain::SourceOnly,
            (true, true) => ExprColumnDomain::Mixed,
        }
    }

    fn remap_expr_for_side(
        expr: &Arc<dyn PhysicalExpr>,
        offset: usize,
        valid_len: usize,
        total_len: usize,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.clone()
            .transform_up(|node| {
                if let Some(column) = node.as_any().downcast_ref::<Column>() {
                    if column.index() >= total_len {
                        return Err(DataFusionError::Plan(format!(
                            "Join condition references column index {} beyond schema with {} fields",
                            column.index(),
                            total_len
                        )));
                    }
                    if column.index() < offset || column.index() >= offset + valid_len {
                        return Err(DataFusionError::Plan(format!(
                            "Join condition column index {} is outside the bounds of the expected join input range [{}, {})",
                            column.index(),
                            offset,
                            offset + valid_len
                        )));
                    }
                    let updated_idx = column.index() - offset;
                    if updated_idx != column.index() {
                        let updated = Arc::new(Column::new(column.name(), updated_idx))
                            as Arc<dyn PhysicalExpr>;
                        return Ok(Transformed::yes(updated));
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }

    fn combine_conjunction(exprs: &[Arc<dyn PhysicalExpr>]) -> Option<Arc<dyn PhysicalExpr>> {
        let mut iter = exprs.iter().cloned();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| {
            Arc::new(BinaryExpr::new(acc, Operator::And, expr)) as Arc<dyn PhysicalExpr>
        }))
    }

    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store = self
            .session
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let storage_config = StorageConfig;
        let table =
            open_table_with_object_store(self.table_url.clone(), object_store, storage_config)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let log_store = table.log_store();
        let snapshot_state = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .clone();
        let table_schema = snapshot_state
            .snapshot()
            .arrow_schema()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let partition_columns = snapshot_state.metadata().partition_columns().clone();

        // Enable schema evolution if requested by MERGE options.
        let mut options = self.options.clone();
        if self.merge_info.with_schema_evolution {
            options.merge_schema = true;
        }

        let join_analysis = self.analyze_join_condition()?;

        let target_plan = self
            .build_target_plan(
                snapshot_state.clone(),
                log_store.clone(),
                &join_analysis.target_only_filters,
            )
            .await?;
        let source_plan = self.augment_source_plan(Arc::clone(&self.merge_info.source))?;
        let target_physical_schema = target_plan.schema();
        let source_physical_schema = source_plan.schema();

        let target_physical_fields: Vec<String> = target_physical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let source_physical_fields: Vec<String> = source_physical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        dbg!(("merge_target_physical_fields", &target_physical_fields));
        dbg!(("merge_source_physical_fields", &source_physical_fields));

        let join_for_ident = self.build_join_plan(
            Arc::clone(&target_plan),
            Arc::clone(&source_plan),
            &join_analysis,
        )?;

        let touched_plan = self.build_touched_file_plan(
            Arc::clone(&join_for_ident),
            Arc::clone(&target_physical_schema),
            Arc::clone(&source_physical_schema),
        )?;

        let touched_paths = if let Some(plan) = touched_plan {
            self.collect_touched_paths(plan).await?
        } else {
            HashSet::new()
        };

        let touched_adds = self
            .load_add_actions_for_paths(&snapshot_state, &log_store, &touched_paths)
            .await?;

        let join_plan =
            self.build_join_plan(target_plan, Arc::clone(&source_plan), &join_analysis)?;

        let filtered = self.build_merge_row_filter(
            join_plan,
            Arc::clone(&target_physical_schema),
            Arc::clone(&source_physical_schema),
        )?;
        let filtered =
            self.filter_to_touched_rows(filtered, &target_physical_schema, &touched_paths)?;
        let projected = self.build_merge_projection(
            filtered,
            target_physical_schema,
            source_physical_schema,
            table_schema.clone(),
        )?;

        let writer = Arc::new(DeltaWriterExec::new(
            Arc::clone(&projected),
            self.table_url.clone(),
            options,
            partition_columns.clone(),
            PhysicalSinkMode::Append,
            true,
            table_schema.clone(),
            None,
        ));

        let mut action_inputs: Vec<Arc<dyn ExecutionPlan>> =
            vec![Arc::clone(&writer) as Arc<dyn ExecutionPlan>];

        if !touched_adds.is_empty() {
            let remove_source = self.create_add_actions_plan(&touched_adds).await?;
            let remove_plan = Arc::new(DeltaRemoveActionsExec::new(remove_source));
            action_inputs.push(remove_plan);
        }

        let commit_input: Arc<dyn ExecutionPlan> = if action_inputs.len() == 1 {
            writer
        } else {
            UnionExec::try_new(action_inputs)?
        };

        let commit = Arc::new(DeltaCommitExec::new(
            commit_input,
            self.table_url.clone(),
            partition_columns,
            true, // table exists
            table_schema,
            PhysicalSinkMode::Append,
        ));

        Ok(commit)
    }

    async fn build_target_plan(
        &self,
        snapshot_state: DeltaTableState,
        log_store: LogStoreRef,
        target_filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan_config = DeltaScanConfigBuilder::new()
            .with_file_column(true)
            .build(&snapshot_state)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider = DeltaTableProvider::try_new(snapshot_state, log_store, scan_config)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut plan = provider
            .scan(self.session, None, &[], None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for filter in target_filters {
            let aligned = self.align_filter_to_target_schema(filter, &plan.schema())?;
            plan = Arc::new(FilterExec::try_new(aligned, plan)?);
        }

        Ok(plan)
    }

    fn align_filter_to_target_schema(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.clone()
            .transform_up(|node| {
                if let Some(col) = node.as_any().downcast_ref::<Column>() {
                    if let Ok(idx) = schema.index_of(col.name()) {
                        if idx != col.index() {
                            let updated =
                                Arc::new(Column::new(col.name(), idx)) as Arc<dyn PhysicalExpr>;
                            return Ok(Transformed::yes(updated));
                        }
                    }
                }
                Ok(Transformed::no(node))
            })
            .data()
    }

    fn augment_source_plan(
        &self,
        source: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let logical_schema = self.merge_info.source_schema.as_ref().as_arrow().clone();
        let logical_field_names: Vec<String> = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let logical_metadata: Vec<HashMap<String, String>> = logical_schema
            .fields()
            .iter()
            .map(|f| f.metadata().clone())
            .collect();
        let physical_names: Vec<String> = source
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        for (idx, field) in source.schema().fields().iter().enumerate() {
            let name = logical_schema
                .fields()
                .get(idx)
                .map(|f| f.name().clone())
                .unwrap_or_else(|| field.name().clone());
            projection_exprs.push((
                Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                name,
            ));
        }
        let projected_names: Vec<String> = projection_exprs
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
        dbg!(("merge_augment_source_logical_fields", &logical_field_names));
        dbg!(("merge_augment_source_logical_metadata", &logical_metadata));
        dbg!(("merge_augment_source_physical_fields", &physical_names));
        dbg!(("merge_augment_source_output_fields", &projected_names));

        projection_exprs.push((
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))) as Arc<dyn PhysicalExpr>,
            SOURCE_PRESENT_COLUMN.to_string(),
        ));

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, source)?))
    }

    fn build_join_plan(
        &self,
        target: Arc<dyn ExecutionPlan>,
        source: Arc<dyn ExecutionPlan>,
        join_analysis: &JoinConditionAnalysis,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !join_analysis.join_keys.is_empty() {
            let filter = if let Some(expr) = &join_analysis.residual_filter {
                Some(self.build_join_filter(expr.clone())?)
            } else {
                None
            };

            let join = HashJoinExec::try_new(
                target,
                source,
                join_analysis.join_keys.clone(),
                filter,
                &JoinType::Full,
                None,
                PartitionMode::Auto,
                NullEquality::NullEqualsNothing,
            )?;
            Ok(Arc::new(join))
        } else {
            let filter = self.build_join_filter(Arc::clone(&self.merge_info.on_condition))?;
            let join =
                NestedLoopJoinExec::try_new(target, source, Some(filter), &JoinType::Full, None)?;
            Ok(Arc::new(join))
        }
    }

    fn build_join_filter(&self, condition: Arc<dyn PhysicalExpr>) -> Result<JoinFilter> {
        let mut column_indices = Vec::new();
        let mut fields = Vec::new();

        let target_arrow = self.merge_info.target_schema.as_ref().as_arrow().clone();
        for (idx, field) in target_arrow.fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            column_indices.push(ColumnIndex {
                index: idx,
                side: JoinSide::Left,
            });
        }

        let source_arrow = self.merge_info.source_schema.as_ref().as_arrow().clone();
        for (idx, field) in source_arrow.fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            column_indices.push(ColumnIndex {
                index: idx,
                side: JoinSide::Right,
            });
        }

        let schema = Arc::new(Schema::new(fields));
        Ok(JoinFilter::new(condition, column_indices, schema))
    }

    fn filter_to_touched_rows(
        &self,
        input: Arc<dyn ExecutionPlan>,
        target_physical_schema: &SchemaRef,
        touched_paths: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_fields = target_physical_schema.fields();
        let path_idx = target_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == PATH_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    PATH_COLUMN
                ))
            })?;

        let path_col = Arc::new(Column::new(PATH_COLUMN, path_idx)) as Arc<dyn PhysicalExpr>;
        let target_present =
            Arc::new(IsNotNullExpr::new(path_col.clone())) as Arc<dyn PhysicalExpr>;
        let touched_literals: Vec<Arc<dyn PhysicalExpr>> = touched_paths
            .iter()
            .map(|path| {
                Arc::new(Literal::new(ScalarValue::Utf8(Some(path.clone()))))
                    as Arc<dyn PhysicalExpr>
            })
            .collect();
        let path_in_touched: Arc<dyn PhysicalExpr> = if touched_literals.is_empty() {
            Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
        } else {
            Arc::new(InListExpr::new(
                path_col.clone(),
                touched_literals,
                false,
                None,
            ))
        };
        let touched_targets = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            path_in_touched,
        )) as Arc<dyn PhysicalExpr>;
        let source_only = Arc::new(NotExpr::new(target_present.clone())) as Arc<dyn PhysicalExpr>;
        let keep_pred = Arc::new(BinaryExpr::new(touched_targets, Operator::Or, source_only))
            as Arc<dyn PhysicalExpr>;

        Ok(Arc::new(FilterExec::try_new(keep_pred, input)?))
    }

    fn build_touched_file_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        target_physical_schema: SchemaRef,
        source_physical_schema: SchemaRef,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let target_fields = target_physical_schema.fields();
        let source_fields = source_physical_schema.fields();
        let num_target = target_fields.len();
        let path_idx = target_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == PATH_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    PATH_COLUMN
                ))
            })?;
        let source_present_idx_in_source = source_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == SOURCE_PRESENT_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    SOURCE_PRESENT_COLUMN
                ))
            })?;
        let src_present_idx = num_target + source_present_idx_in_source;

        let path_col = Arc::new(Column::new(PATH_COLUMN, path_idx)) as Arc<dyn PhysicalExpr>;
        let source_present = Arc::new(IsNotNullExpr::new(Arc::new(Column::new(
            SOURCE_PRESENT_COLUMN,
            src_present_idx,
        )) as Arc<dyn PhysicalExpr>)) as Arc<dyn PhysicalExpr>;
        let target_present =
            Arc::new(IsNotNullExpr::new(path_col.clone())) as Arc<dyn PhysicalExpr>;
        let matched_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let not_source_present =
            Arc::new(NotExpr::new(source_present.clone())) as Arc<dyn PhysicalExpr>;
        let not_matched_by_source_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            not_source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let mut rewrite_pred: Option<Arc<dyn PhysicalExpr>> = None;
        let accumulate = |existing: &mut Option<Arc<dyn PhysicalExpr>>,
                          expr: Arc<dyn PhysicalExpr>| {
            *existing = Some(match existing.take() {
                Some(prev) => {
                    Arc::new(BinaryExpr::new(prev, Operator::Or, expr)) as Arc<dyn PhysicalExpr>
                }
                None => expr,
            });
        };

        for clause in &self.merge_info.matched_clauses {
            let mut pred = matched_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }
            use sail_common_datafusion::datasource::MergeMatchedActionInfo as MMAI;
            match &clause.action {
                MMAI::Delete | MMAI::UpdateAll | MMAI::UpdateSet(_) => {
                    accumulate(&mut rewrite_pred, pred);
                }
            }
        }

        for clause in &self.merge_info.not_matched_by_source_clauses {
            let mut pred = not_matched_by_source_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }
            use sail_common_datafusion::datasource::MergeNotMatchedBySourceActionInfo as NMBAI;
            match &clause.action {
                NMBAI::Delete | NMBAI::UpdateSet(_) => {
                    accumulate(&mut rewrite_pred, pred);
                }
            }
        }

        if rewrite_pred.is_none() {
            return Ok(None);
        }

        let filter = Arc::new(FilterExec::try_new(rewrite_pred.unwrap(), input)?);
        let projection =
            ProjectionExec::try_new(vec![(path_col, PATH_COLUMN.to_string())], filter)?;
        Ok(Some(Arc::new(projection)))
    }

    /// Build a row-level filter that keeps:
    /// - target rows that are not deleted, and
    /// - source-only rows that are inserted.
    fn build_merge_row_filter(
        &self,
        input: Arc<dyn ExecutionPlan>,
        target_physical_schema: SchemaRef,
        source_physical_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_fields = target_physical_schema.fields();
        let source_fields = source_physical_schema.fields();
        let num_target = target_fields.len();
        let path_idx = target_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == PATH_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    PATH_COLUMN
                ))
            })?;

        let source_present_idx_in_source = source_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == SOURCE_PRESENT_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    SOURCE_PRESENT_COLUMN
                ))
            })?;
        let src_present_idx = num_target + source_present_idx_in_source;

        let path_col = Arc::new(Column::new(PATH_COLUMN, path_idx)) as Arc<dyn PhysicalExpr>;
        let source_present_col =
            Arc::new(Column::new(SOURCE_PRESENT_COLUMN, src_present_idx)) as Arc<dyn PhysicalExpr>;

        let target_present =
            Arc::new(IsNotNullExpr::new(path_col.clone())) as Arc<dyn PhysicalExpr>;
        let source_present =
            Arc::new(IsNotNullExpr::new(source_present_col.clone())) as Arc<dyn PhysicalExpr>;

        let matched_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let not_source_present =
            Arc::new(NotExpr::new(source_present.clone())) as Arc<dyn PhysicalExpr>;
        let not_target_present =
            Arc::new(NotExpr::new(target_present.clone())) as Arc<dyn PhysicalExpr>;

        let not_matched_by_source_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            not_source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let not_matched_by_target_pred = Arc::new(BinaryExpr::new(
            not_target_present.clone(),
            Operator::And,
            source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let mut delete_pred: Option<Arc<dyn PhysicalExpr>> = None;
        let mut insert_pred: Option<Arc<dyn PhysicalExpr>> = None;

        let or_expr =
            |left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
                Arc::new(BinaryExpr::new(left, Operator::Or, right)) as Arc<dyn PhysicalExpr>
            };

        // Matched clauses
        for clause in &self.merge_info.matched_clauses {
            let mut pred = matched_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }

            use sail_common_datafusion::datasource::MergeMatchedActionInfo as MMAI;
            match &clause.action {
                MMAI::Delete => {
                    delete_pred = Some(match delete_pred {
                        Some(existing) => or_expr(existing, pred),
                        None => pred,
                    });
                }
                MMAI::UpdateAll | MMAI::UpdateSet(_) => {}
            }
        }

        // NOT MATCHED BY SOURCE clauses
        for clause in &self.merge_info.not_matched_by_source_clauses {
            let mut pred = not_matched_by_source_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }

            use sail_common_datafusion::datasource::MergeNotMatchedBySourceActionInfo as NMBAI;
            match &clause.action {
                NMBAI::Delete => {
                    delete_pred = Some(match delete_pred {
                        Some(existing) => or_expr(existing, pred),
                        None => pred,
                    });
                }
                NMBAI::UpdateSet(_) => {}
            }
        }

        // NOT MATCHED BY TARGET clauses (insert-only)
        for clause in &self.merge_info.not_matched_by_target_clauses {
            let mut pred = not_matched_by_target_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }

            use sail_common_datafusion::datasource::MergeNotMatchedByTargetActionInfo as NMTI;
            match &clause.action {
                NMTI::InsertAll | NMTI::InsertColumns { .. } => {
                    insert_pred = Some(match insert_pred {
                        Some(existing) => or_expr(existing, pred),
                        None => pred,
                    });
                }
            }
        }

        let false_lit =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))) as Arc<dyn PhysicalExpr>;
        let delete_expr = delete_pred.unwrap_or_else(|| false_lit.clone());
        let insert_expr = insert_pred.unwrap_or_else(|| false_lit.clone());

        let not_delete = Arc::new(NotExpr::new(delete_expr)) as Arc<dyn PhysicalExpr>;
        let keep_or_update = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            not_delete,
        )) as Arc<dyn PhysicalExpr>;

        let active_expr = Arc::new(BinaryExpr::new(keep_or_update, Operator::Or, insert_expr))
            as Arc<dyn PhysicalExpr>;

        dbg!(("build_merge_row_filter active_expr", &active_expr));

        let filter = FilterExec::try_new(active_expr, input)?;
        Ok(Arc::new(filter))
    }

    /// Build the projection that computes final table columns after applying
    /// MERGE clauses. Rows have already been filtered for delete/insert
    /// semantics by `build_merge_row_filter`.
    fn build_merge_projection(
        &self,
        input: Arc<dyn ExecutionPlan>,
        target_physical_schema: SchemaRef,
        source_physical_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let target_fields = target_physical_schema.fields();
        let source_fields = source_physical_schema.fields();
        let num_target = target_fields.len();
        let target_field_names: Vec<String> =
            target_fields.iter().map(|f| f.name().clone()).collect();
        let source_field_names: Vec<String> =
            source_fields.iter().map(|f| f.name().clone()).collect();
        let table_field_names: Vec<String> = table_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        dbg!(("merge_projection_target_fields", &target_field_names));
        dbg!(("merge_projection_source_fields", &source_field_names));
        dbg!(("merge_projection_table_fields", &table_field_names));

        // Logical MERGE assignments can refer to normalized column names (e.g. "#1")
        // while the table schema uses the actual column names. Keep an index-based
        // mapping between logical target schema fields and the physical table schema.
        let logical_target_schema = self.merge_info.target_schema.as_ref().as_arrow();
        let mut logical_to_physical: HashMap<String, String> = HashMap::new();
        if logical_target_schema.fields().len() == table_schema.fields().len() {
            for (idx, logical_field) in logical_target_schema.fields().iter().enumerate() {
                let physical_field = table_schema.field(idx);
                logical_to_physical
                    .insert(logical_field.name().clone(), physical_field.name().clone());
            }
        }

        let path_idx = target_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == PATH_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    PATH_COLUMN
                ))
            })?;

        let source_present_idx_in_source = source_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == SOURCE_PRESENT_COLUMN)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Column '{}' not found in MERGE join schema",
                    SOURCE_PRESENT_COLUMN
                ))
            })?;
        let src_present_idx = num_target + source_present_idx_in_source;

        let path_col = Arc::new(Column::new(PATH_COLUMN, path_idx)) as Arc<dyn PhysicalExpr>;
        let source_present_col =
            Arc::new(Column::new(SOURCE_PRESENT_COLUMN, src_present_idx)) as Arc<dyn PhysicalExpr>;

        let target_present =
            Arc::new(IsNotNullExpr::new(path_col.clone())) as Arc<dyn PhysicalExpr>;
        let source_present =
            Arc::new(IsNotNullExpr::new(source_present_col.clone())) as Arc<dyn PhysicalExpr>;

        let matched_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let not_source_present =
            Arc::new(NotExpr::new(source_present.clone())) as Arc<dyn PhysicalExpr>;
        let not_target_present =
            Arc::new(NotExpr::new(target_present.clone())) as Arc<dyn PhysicalExpr>;

        let not_matched_by_source_pred = Arc::new(BinaryExpr::new(
            target_present.clone(),
            Operator::And,
            not_source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let not_matched_by_target_pred = Arc::new(BinaryExpr::new(
            not_target_present.clone(),
            Operator::And,
            source_present.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let mut target_idx_by_name: HashMap<String, usize> = HashMap::new();
        for (idx, field) in target_fields.iter().enumerate() {
            target_idx_by_name.insert(field.name().clone(), idx);
        }
        let mut source_idx_by_name: HashMap<String, usize> = HashMap::new();
        for (idx, field) in source_fields.iter().enumerate() {
            source_idx_by_name.insert(field.name().clone(), num_target + idx);
        }

        dbg!(("merge_projection_target_idx_by_name", &target_idx_by_name));

        // Precompute target/source column expressions for each output column.
        // Target expressions are resolved by column name, while source expressions
        // are resolved by ordinal position to handle unnamed source columns like "#3".
        let mut target_exprs: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
        let mut source_exprs: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
        let num_source = source_fields.len();

        for (i, field) in table_schema.fields().iter().enumerate() {
            let name = field.name().clone();
            let data_type = field.data_type().clone();

            let target_expr: Arc<dyn PhysicalExpr> =
                if let Some(idx) = target_idx_by_name.get(&name) {
                    Arc::new(Column::new(&name, *idx)) as Arc<dyn PhysicalExpr>
                } else {
                    let typed_null =
                        ScalarValue::try_from(&data_type).map_err(DataFusionError::from)?;
                    Arc::new(Literal::new(typed_null)) as Arc<dyn PhysicalExpr>
                };
            let source_expr: Arc<dyn PhysicalExpr> = if i < num_source {
                let source_field = &source_fields[i];
                let source_name = source_field.name();
                let source_idx = num_target + i;
                Arc::new(Column::new(source_name, source_idx)) as Arc<dyn PhysicalExpr>
            } else {
                let typed_null =
                    ScalarValue::try_from(&data_type).map_err(DataFusionError::from)?;
                Arc::new(Literal::new(typed_null)) as Arc<dyn PhysicalExpr>
            };

            target_exprs.insert(name.clone(), target_expr);
            source_exprs.insert(name.clone(), source_expr);
        }

        // For each output column, collect CASE when/then branches
        let mut column_cases: HashMap<String, Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> =
            HashMap::new();
        for field in table_schema.fields() {
            column_cases.insert(field.name().clone(), Vec::new());
        }

        // Matched clauses
        for clause in &self.merge_info.matched_clauses {
            let mut pred = matched_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }
            let pred = Self::align_expr_columns(
                &pred,
                &input_schema,
                &target_idx_by_name,
                &source_idx_by_name,
                &logical_to_physical,
            )?;

            use sail_common_datafusion::datasource::{
                MergeAssignmentInfo, MergeMatchedActionInfo as MMAI,
            };
            match &clause.action {
                MMAI::Delete => {
                    // Values are irrelevant for deleted rows (already filtered out)
                }
                MMAI::UpdateAll => {
                    for field in table_schema.fields() {
                        let name = field.name();
                        let src_expr = source_exprs.get(name).cloned().unwrap_or_else(|| {
                            let typed_null = ScalarValue::try_from(field.data_type())
                                .map_err(DataFusionError::from)
                                .unwrap();
                            Arc::new(Literal::new(typed_null)) as Arc<dyn PhysicalExpr>
                        });
                        if let Some(cases) = column_cases.get_mut(name) {
                            cases.push((pred.clone(), src_expr));
                        }
                    }
                }
                MMAI::UpdateSet(assignments) => {
                    let mut assign_map: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
                    for MergeAssignmentInfo { column, value } in assignments {
                        let phys_name = logical_to_physical
                            .get(column.as_str())
                            .cloned()
                            .unwrap_or_else(|| column.clone());
                        let aligned_value = Self::align_expr_columns(
                            value,
                            &input_schema,
                            &target_idx_by_name,
                            &source_idx_by_name,
                            &logical_to_physical,
                        )?;
                        assign_map.insert(phys_name, aligned_value);
                    }
                    dbg!(("UpdateSet assignments", &assign_map));
                    for (col, value_expr) in assign_map {
                        if let Some(cases) = column_cases.get_mut(col.as_str()) {
                            dbg!(("Pushing UpdateSet case", col.as_str(), &value_expr));
                            cases.push((pred.clone(), Arc::clone(&value_expr)));
                        }
                    }
                }
            }
        }

        // NOT MATCHED BY SOURCE clauses
        for clause in &self.merge_info.not_matched_by_source_clauses {
            let mut pred = not_matched_by_source_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }
            let pred = Self::align_expr_columns(
                &pred,
                &input_schema,
                &target_idx_by_name,
                &source_idx_by_name,
                &logical_to_physical,
            )?;

            use sail_common_datafusion::datasource::{
                MergeAssignmentInfo, MergeNotMatchedBySourceActionInfo as NMBAI,
            };
            match &clause.action {
                NMBAI::Delete => {
                    // Values are irrelevant for deleted rows (already filtered out)
                }
                NMBAI::UpdateSet(assignments) => {
                    let mut assign_map: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
                    for MergeAssignmentInfo { column, value } in assignments {
                        let phys_name = logical_to_physical
                            .get(column.as_str())
                            .cloned()
                            .unwrap_or_else(|| column.clone());
                        let aligned_value = Self::align_expr_columns(
                            value,
                            &input_schema,
                            &target_idx_by_name,
                            &source_idx_by_name,
                            &logical_to_physical,
                        )?;
                        assign_map.insert(phys_name, aligned_value);
                    }
                    for (col, value_expr) in assign_map {
                        if let Some(cases) = column_cases.get_mut(col.as_str()) {
                            cases.push((pred.clone(), Arc::clone(&value_expr)));
                        }
                    }
                }
            }
        }

        // NOT MATCHED BY TARGET clauses (insert-only)
        for clause in &self.merge_info.not_matched_by_target_clauses {
            let mut pred = not_matched_by_target_pred.clone();
            if let Some(cond) = &clause.condition {
                pred = Arc::new(BinaryExpr::new(pred, Operator::And, Arc::clone(cond)))
                    as Arc<dyn PhysicalExpr>;
            }
            let pred = Self::align_expr_columns(
                &pred,
                &input_schema,
                &target_idx_by_name,
                &source_idx_by_name,
                &logical_to_physical,
            )?;

            use sail_common_datafusion::datasource::MergeNotMatchedByTargetActionInfo as NMTI;
            match &clause.action {
                NMTI::InsertAll => {
                    for field in table_schema.fields() {
                        let name = field.name();
                        let src_expr = source_exprs.get(name).cloned().unwrap_or_else(|| {
                            let typed_null = ScalarValue::try_from(field.data_type())
                                .map_err(DataFusionError::from)
                                .unwrap();
                            Arc::new(Literal::new(typed_null)) as Arc<dyn PhysicalExpr>
                        });
                        if let Some(cases) = column_cases.get_mut(name) {
                            cases.push((pred.clone(), src_expr));
                        }
                    }
                }
                NMTI::InsertColumns { columns, values } => {
                    for (col, value_expr) in columns.iter().zip(values.iter()) {
                        let phys_name = logical_to_physical
                            .get(col.as_str())
                            .cloned()
                            .unwrap_or_else(|| col.clone());
                        let aligned_value = Self::align_expr_columns(
                            value_expr,
                            &input_schema,
                            &target_idx_by_name,
                            &source_idx_by_name,
                            &logical_to_physical,
                        )?;
                        if let Some(cases) = column_cases.get_mut(phys_name.as_str()) {
                            cases.push((pred.clone(), aligned_value));
                        }
                    }
                }
            }
        }

        // Build projection expressions for final table columns
        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        for field in table_schema.fields() {
            let name = field.name().clone();
            let default_expr = target_exprs.get(&name).cloned().unwrap_or_else(|| {
                let typed_null = ScalarValue::try_from(field.data_type())
                    .map_err(DataFusionError::from)
                    .unwrap();
                Arc::new(Literal::new(typed_null)) as Arc<dyn PhysicalExpr>
            });

            let cases = column_cases.remove(&name).unwrap_or_default();
            let expr: Arc<dyn PhysicalExpr> = if cases.is_empty() {
                default_expr
            } else {
                Arc::new(CaseExpr::try_new(None, cases, Some(default_expr))?)
                    as Arc<dyn PhysicalExpr>
            };

            projection_exprs.push((expr, name));
        }

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
    }

    fn align_expr_columns(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
        target_idx_by_name: &HashMap<String, usize>,
        source_idx_by_name: &HashMap<String, usize>,
        logical_to_physical: &HashMap<String, String>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let schema = Arc::clone(schema);
        expr.clone()
            .transform_up(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    let mut target_name: Option<String> = None;
                    let name = col.name();
                    let mut new_index: Option<usize> = None;

                    if let Some(mapped) = logical_to_physical.get(name) {
                        target_name = Some(mapped.clone());
                        new_index = target_idx_by_name.get(mapped).cloned();
                    } else if let Some(idx) = target_idx_by_name.get(name) {
                        new_index = Some(*idx);
                    } else if let Some(idx) = source_idx_by_name.get(name) {
                        new_index = Some(*idx);
                    } else if let Some(idx) = schema.fields().iter().position(|f| f.name() == name)
                    {
                        new_index = Some(idx);
                        target_name = Some(schema.field(idx).name().clone());
                    }

                    if let Some(idx) = new_index {
                        let final_name = target_name.unwrap_or_else(|| name.to_string());
                        if idx != col.index() || final_name != name {
                            dbg!((
                                "align_expr_columns remap",
                                name,
                                col.index(),
                                &final_name,
                                idx
                            ));
                            let updated = Arc::new(Column::new(final_name.as_str(), idx))
                                as Arc<dyn PhysicalExpr>;
                            return Ok(Transformed::yes(updated));
                        }
                    }
                }
                Ok(Transformed::no(e))
            })
            .data()
    }

    async fn collect_touched_paths(&self, plan: Arc<dyn ExecutionPlan>) -> Result<HashSet<String>> {
        let task_ctx = self.session.task_ctx();
        let batches = collect(plan, task_ctx)
            .await
            .map_err(DataFusionError::from)?;
        let mut paths = HashSet::new();
        for batch in batches {
            if batch.num_columns() == 0 {
                continue;
            }
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Touched file plan must yield a Utf8 path column".to_string(),
                    )
                })?;
            for value in array.iter().flatten() {
                paths.insert(value.to_string());
            }
        }
        Ok(paths)
    }

    async fn load_add_actions_for_paths(
        &self,
        snapshot_state: &DeltaTableState,
        log_store: &LogStoreRef,
        paths: &HashSet<String>,
    ) -> Result<Vec<Add>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let mut stream = snapshot_state.snapshot().files(log_store.as_ref(), None);
        let mut adds = Vec::new();
        while let Some(view) = stream
            .try_next()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            let add = view.add_action();
            if paths.contains(&add.path) {
                adds.push(add);
            }
        }
        Ok(adds)
    }

    async fn create_add_actions_plan(&self, adds: &[Add]) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("add", DataType::Utf8, true),
            Field::new("partition_scan", DataType::Boolean, false),
        ]));

        let batch = if adds.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            let add_strings: Result<Vec<Option<String>>> = adds
                .iter()
                .map(|add| {
                    serde_json::to_string(add)
                        .map(Some)
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .collect();
            let add_array = Arc::new(StringArray::from(add_strings?));
            let bools = vec![false; adds.len()];
            let partition_scan = Arc::new(BooleanArray::from(bools));
            RecordBatch::try_new(schema.clone(), vec![add_array, partition_scan])
                .map_err(DataFusionError::from)?
        };

        let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(DataFusionError::from)?;
        table
            .scan(self.session, None, &[], None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
