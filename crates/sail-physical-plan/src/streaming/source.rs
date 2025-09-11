use std::any::Any;
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{plan_err, Result};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::{
    EncodedFlowEventStream, FlowEvent, FlowEventStreamAdapter, FlowMarker,
};
use sail_common_datafusion::streaming::schema::to_flow_event_schema;

#[derive(Debug)]
pub struct StreamSourceAdapterExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl StreamSourceAdapterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = Arc::new(to_flow_event_schema(&input.schema()));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self { input, properties }
    }
}

impl DisplayAs for StreamSourceAdapterExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamSourceAdapterExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        Ok(Arc::new(StreamSourceAdapterExec::new(child)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self
            .input
            .execute(partition, context)?
            .map(|x| Ok(FlowEvent::append_only_data(x?)))
            .chain(stream::once(async {
                Ok(FlowEvent::Marker(FlowMarker::EndOfData))
            }));
        let stream = Box::pin(FlowEventStreamAdapter::new(self.input.schema(), stream));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}
