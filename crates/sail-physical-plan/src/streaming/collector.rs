use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, internal_err, plan_err};
use futures::TryStreamExt;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::DecodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;

use super::state::{RowMultiset, row_at, rows_to_batch};

/// A physical plan node that collects a stream of retractable data batches
/// into final data batches.
/// The input schema must be a flow event schema, while the output schema
/// is the corresponding data schema.
#[derive(Debug)]
pub struct StreamCollectorExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StreamCollectorExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if input.properties().boundedness != Boundedness::Bounded {
            return plan_err!("stream collector requires bounded input");
        }
        let schema = Arc::new(try_from_flow_event_schema(&input.schema())?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            // We emit data at the end since we need to handle retractions.
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self { input, properties })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for StreamCollectorExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamCollectorExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(StreamCollectorExec::try_new(child)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("invalid partition for {}: {partition}", self.name());
        }

        if self.input.output_partitioning().partition_count() != 1 {
            return internal_err!("{} requires a single input partition", self.name());
        }
        let stream = self.input.execute(partition, context.clone())?;
        let schema = self.schema();
        let output_schema = schema.clone();
        let output = futures::stream::once(async move {
            let mut stream = DecodedFlowEventStream::try_new(stream)?;
            let mut state = RowMultiset::default();
            let mut memory =
                MemoryConsumer::new("StreamCollectorExec").register(context.memory_pool());
            while let Some(event) = stream.try_next().await? {
                if let FlowEvent::Data { batch, retracted } = event {
                    for i in 0..batch.num_rows() {
                        state.apply(row_at(&batch, i)?, retracted.value(i), &mut memory)?;
                    }
                }
            }
            Ok::<_, datafusion_common::DataFusionError>(futures::stream::try_unfold(
                (state.into_rows(), memory),
                move |(mut rows, memory)| {
                    let chunk = rows.by_ref().take(1024).collect::<Vec<_>>();
                    let result = if chunk.is_empty() {
                        Ok(None)
                    } else {
                        rows_to_batch(schema.clone(), &chunk)
                            .map(|batch| Some((batch, (rows, memory))))
                    };
                    futures::future::ready(result)
                },
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            output,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}
