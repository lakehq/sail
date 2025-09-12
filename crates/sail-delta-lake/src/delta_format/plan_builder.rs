use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_expr::{LexRequirement, PhysicalExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::{
    create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaFindFilesExec,
    DeltaWriterExec,
};
use crate::delta_datafusion::{
    delta_to_datafusion_error, DataFusionMixins, DeltaScanConfigBuilder, DeltaTableProvider,
};
use crate::options::TableDeltaOptions;

/// Builder for creating a Delta Lake execution plan with the specified structure:
/// Input -> Project -> Repartition -> Sort -> CoalescePartitions -> Writer -> Commit
/// For OverwriteIf mode: NewData + OldData -> Union -> Writer -> Commit
pub struct DeltaPlanBuilder<'a> {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_schema_for_cond: Option<SchemaRef>,
    table_exists: bool,
    sort_order: Option<LexRequirement>,
    session_state: &'a dyn Session,
}

impl<'a> DeltaPlanBuilder<'a> {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_schema_for_cond: Option<SchemaRef>,
        table_exists: bool,
        sort_order: Option<LexRequirement>,
        session_state: &'a dyn Session,
    ) -> Self {
        Self {
            input,
            table_url,
            options,
            partition_columns,
            sink_mode,
            table_schema_for_cond,
            table_exists,
            sort_order,
            session_state,
        }
    }

    /// Build the complete execution plan chain
    pub async fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let current_plan = match self.sink_mode.clone() {
            PhysicalSinkMode::OverwriteIf { condition } => self.build_union_plan(condition).await?,
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
            .and_then(|plan| self.add_commit_node(plan, None))
    }

    /// Build a Union execution plan for OverwriteIf mode
    async fn build_union_plan(
        self,
        condition: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let remove_plan = {
            let table_schema = self.table_schema_for_cond.clone();
            Some(Arc::new(DeltaFindFilesExec::new(
                self.table_url.clone(),
                Some(condition.clone()),
                table_schema,
            )) as Arc<dyn ExecutionPlan>)
        };

        let old_data_plan = self.build_old_data_plan(condition).await?;

        let new_data_plan = self
            .add_projection_node(self.input.clone())
            .and_then(|plan| self.add_repartition_node(plan))
            .and_then(|plan| self.add_sort_node(plan))?;

        self.align_schemas(new_data_plan, old_data_plan)
            .map(|(aligned_new_data, aligned_old_data)| {
                Arc::new(UnionExec::new(vec![aligned_new_data, aligned_old_data]))
                    as Arc<dyn ExecutionPlan>
            })
            .and_then(|union_plan| self.add_writer_node(union_plan))
            .and_then(|plan| self.add_commit_node(plan, remove_plan))
    }

    /// Build the plan for scanning and filtering old data
    async fn build_old_data_plan(
        &self,
        condition: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create object store and load table using the session context
        let object_store = self
            .session_state
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        let log_store = deltalake::logstore::default_logstore(
            object_store.clone(),
            object_store,
            &self.table_url,
            &Default::default(),
        );

        let mut table = crate::table::DeltaTable::new(log_store, Default::default());
        table
            .load()
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create table provider for scanning existing data
        let snapshot_state = table.snapshot().map_err(delta_to_datafusion_error)?;

        let scan_config = DeltaScanConfigBuilder::new()
            .with_schema(
                snapshot_state
                    .arrow_schema()
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?,
            )
            .build(snapshot_state)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        let table_provider = Arc::new(
            DeltaTableProvider::try_new(snapshot_state.clone(), table.log_store(), scan_config)
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?,
        );

        // Create logical plan for scanning
        let logical_plan =
            LogicalPlanBuilder::scan("old_data", provider_as_source(table_provider), None)?
                .build()?;

        // Convert to physical plan
        let scan_plan = self
            .session_state
            .create_physical_plan(&logical_plan)
            .await?;

        // Apply NOT condition filter to keep data that doesn't match the condition
        let negated_condition = Arc::new(datafusion::physical_expr::expressions::NotExpr::new(
            condition,
        ));
        let filter_plan = Arc::new(FilterExec::try_new(negated_condition, scan_plan)?);

        Ok(filter_plan)
    }

    /// Align schemas between new data and old data for UnionExec
    fn align_schemas(
        &self,
        new_data_plan: Arc<dyn ExecutionPlan>,
        old_data_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;

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
        Ok(create_projection(input, self.partition_columns.clone())?)
    }

    fn add_repartition_node(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(create_repartition(input, self.partition_columns.clone())?)
    }

    fn add_sort_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        // create_sort handles both partition columns and user-specified sort order
        Ok(create_sort(
            input,
            self.partition_columns.clone(),
            self.sort_order.clone(),
        )?)
    }

    fn add_writer_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        Ok(Arc::new(DeltaWriterExec::new(
            input,
            self.table_url.clone(),
            self.options.clone(),
            self.partition_columns.clone(),
            self.sink_mode.clone(),
            self.table_exists,
            schema,
        )))
    }

    fn add_commit_node(
        &self,
        input: Arc<dyn ExecutionPlan>,
        remove_plan: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeltaCommitExec::new(
            input,
            remove_plan,
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.input.schema(), // Use original input schema for metadata
            self.sink_mode.clone(),
        )))
    }
}
