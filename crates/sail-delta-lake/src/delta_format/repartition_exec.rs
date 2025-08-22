use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_physical_expr::expressions::Column;

/// `DeltaRepartitionExec` is a factory for creating `RepartitionExec` instances
/// for Delta Lake data repartitioning.
pub struct DeltaRepartitionExec;

impl DeltaRepartitionExec {
    pub fn create_repartition(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>,
    ) -> Result<Arc<RepartitionExec>> {
        let partitioning = if partition_columns.is_empty() {
            // No partition columns, ensure some parallelism
            // TODO: Make partition count configurable
            Partitioning::RoundRobinBatch(4)
        } else {
            // Since DeltaProjectExec moves partition columns to the end, we can rely on their positions.
            let schema = input.schema();
            let num_cols = schema.fields().len();
            let num_part_cols = partition_columns.len();
            let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = (num_cols - num_part_cols..num_cols)
                .zip(partition_columns.iter())
                .map(|(idx, name)| Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>)
                .collect();

            // TODO: Partition count should be configurable
            let num_partitions = 4;
            Partitioning::Hash(partition_exprs, num_partitions)
        };

        Ok(Arc::new(RepartitionExec::try_new(input, partitioning)?))
    }
}
