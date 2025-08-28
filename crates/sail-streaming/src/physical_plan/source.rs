use std::any::Any;
use std::iter::once;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{plan_err, Result};
use futures::StreamExt;

use crate::event::{EncodedFlowEventStream, FlowEvent, FlowEventStreamAdapter};
use crate::field::marker_field;

#[derive(Debug)]
pub struct StreamSourceExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl StreamSourceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let fields = once(Arc::new(marker_field()))
            .chain(input.schema().fields().iter().cloned())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self { input, properties }
    }
}

impl DisplayAs for StreamSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamSourceExec {
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
        Ok(Arc::new(StreamSourceExec::new(child)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self
            .input
            .execute(partition, context)?
            .map(|x| Ok(FlowEvent::Data(x?)));
        let stream = Box::pin(FlowEventStreamAdapter::new(self.input.schema(), stream));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}
