use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, Statistics, internal_err};

/// An optimizer-only fence that keeps Spark-planned file groups opaque to
/// DataFusion's file scan repartitioning.
///
/// Sail's physical optimizer removes this node immediately after
/// `EnforceDistribution`, before job graph planning and remote plan encoding.
#[derive(Debug, Clone)]
pub struct FileScanPartitioningFenceExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl FileScanPartitioningFenceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(input.properties().as_ref().clone());
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for FileScanPartitioningFenceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FileScanPartitioningFenceExec")
    }
}

impl ExecutionPlan for FileScanPartitioningFenceExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    // The input is intentionally hidden until EnforceDistribution has run.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("{} does not expose optimizer children", self.name());
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}
