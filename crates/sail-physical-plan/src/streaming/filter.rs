use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch, RecordBatchOptions, new_null_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, PhysicalExpr};
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties, apply_expression_roots,
};
use datafusion_common::{Result, Statistics};
use futures::TryStreamExt;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;

/// A physical plan node that filters a stream of retractable data batches.
///
/// This is used in streaming plan rewriting to avoid optimizer-inserted repartitions
/// (e.g. `RoundRobinBatch(target_partitions)`) that can make bounded streaming queries
/// unexpectedly slow for small-batch sources.
#[derive(Debug)]
pub struct StreamFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
    properties: Arc<PlanProperties>,
}

impl StreamFilterExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            // Filtering preserves pipeline behavior of input
            input.pipeline_behavior(),
            input.boundedness(),
        ));
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([&self.predicate], f)
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
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

        let flow_schema = self.schema();
        let schema = Arc::new(try_from_flow_event_schema(&flow_schema)?);
        let output_schema = schema.clone();
        let stream = DecodedFlowEventStream::try_new(self.input.execute(partition, context)?)?
            .and_then(move |event| {
                let result = (|| match event {
                    FlowEvent::Marker(marker) => Ok(FlowEvent::Marker(marker)),
                    FlowEvent::Data { batch, retracted } => {
                        let mut columns = vec![
                            new_null_array(&DataType::Binary, batch.num_rows()),
                            Arc::new(retracted) as _,
                        ];
                        columns.extend_from_slice(batch.columns());
                        let batch = RecordBatch::try_new(flow_schema.clone(), columns)?;
                        let batch = batch_filter(&batch, &predicate)?;
                        let retracted = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                datafusion_common::internal_datafusion_err!(
                                    "missing retraction flags after stream filter"
                                )
                            })?
                            .clone();
                        let data = RecordBatch::try_new_with_options(
                            schema.clone(),
                            batch.columns()[2..].to_vec(),
                            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                        )?;
                        Ok(FlowEvent::Data {
                            batch: data,
                            retracted,
                        })
                    }
                })();
                futures::future::ready(result)
            });
        Ok(Box::pin(EncodedFlowEventStream::new(Box::pin(
            FlowEventStreamAdapter::new(output_schema, stream),
        ))))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }
}
