use std::any::Any;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, plan_err, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::encoding::DecodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::FlowEvent;

/// A physical plan node that collects a stream of retractable data batches
/// into final data batches.
/// The input schema must be a flow event schema, while the output schema
/// is the corresponding data schema.
#[derive(Debug)]
pub struct StreamCollectorExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl StreamCollectorExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if input.properties().boundedness != Boundedness::Bounded {
            return plan_err!("stream collector requires bounded input");
        }
        let schema = Arc::new(try_from_flow_event_schema(&input.schema())?);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            // We emit data at the end since we need to handle retractions.
            EmissionType::Final,
            Boundedness::Bounded,
        );
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let stream = self.input.execute(partition, context)?;
        // TODO: collect data batches and handle retractions
        let stream = DecodedFlowEventStream::try_new(stream)?.filter_map(|event| async move {
            match event {
                Ok(FlowEvent::Marker(_)) => None,
                Ok(FlowEvent::Data {
                    batch,
                    retracted: _,
                }) => Some(Ok(batch)),
                Err(e) => Some(Err(e)),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}
