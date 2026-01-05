use std::any::Any;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, Statistics};
use futures::StreamExt;

/// A physical plan node that filters a stream of retractable data batches.
///
/// This is used in streaming plan rewriting to avoid optimizer-inserted repartitions
/// (e.g. `RoundRobinBatch(target_partitions)`) that can make bounded streaming queries
/// unexpectedly slow for small-batch sources.
#[derive(Debug)]
pub struct StreamFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
    properties: PlanProperties,
}

impl StreamFilterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            // Filtering preserves pipeline behavior of input
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Ok(Self {
            input,
            predicate,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }
}

impl DisplayAs for StreamFilterExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamFilterExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "StreamFilterExec requires a child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::try_new(child, Arc::clone(&self.predicate))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let predicate = Arc::clone(&self.predicate);

        let stream = self.input.execute(partition, context)?;
        let stream = stream.map(move |batch| {
            let batch = batch?;
            batch_filter(&batch, &predicate)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }
}
