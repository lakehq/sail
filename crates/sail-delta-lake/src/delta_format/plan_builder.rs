use std::sync::Arc;

use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::{create_projection, create_repartition, create_sort, DeltaCommitExec, DeltaWriterExec};
use crate::options::TableDeltaOptions;

/// Builder for creating a Delta Lake execution plan with the specified structure:
/// Input -> Project -> Repartition -> Sort -> CoalescePartitions -> Writer -> Commit
pub struct DeltaPlanBuilder {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    options: TableDeltaOptions,
    partition_columns: Vec<String>,
    sink_mode: PhysicalSinkMode,
    table_exists: bool,
    sort_order: Option<LexRequirement>,
}

impl DeltaPlanBuilder {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        sink_mode: PhysicalSinkMode,
        table_exists: bool,
        sort_order: Option<LexRequirement>,
    ) -> Self {
        Self {
            input,
            table_url,
            options,
            partition_columns,
            sink_mode,
            table_exists,
            sort_order,
        }
    }

    /// Build the complete execution plan chain
    pub fn build(self) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.input.clone();
        let plan = self.add_projection_node(plan)?;
        let plan = self.add_repartition_node(plan)?;
        let plan = self.add_sort_node(plan)?;
        // TODO: support parallel writes
        let plan = self.add_writer_node(plan)?;
        let plan = self.add_commit_node(plan)?;
        Ok(plan)
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

    fn add_commit_node(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeltaCommitExec::new(
            input,
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.input.schema(), // Use original input schema for metadata
        )))
    }
}
