use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{plan_err, Result};
use futures::StreamExt;
use log::debug;

use crate::event::{DecodedFlowEventStream, FlowEvent};

#[derive(Debug)]
pub struct StreamSinkExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl StreamSinkExec {
    pub fn try_new(inner: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        );
        Ok(Self { inner, properties })
    }
}

impl DisplayAs for StreamSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamSinkExec {
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
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(StreamSinkExec::try_new(child, self.schema())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, context)?;
        let stream = DecodedFlowEventStream::try_new(stream)?.filter_map(async |x| match x {
            Ok(FlowEvent::Data(batch)) => Some(Ok(batch)),
            Ok(FlowEvent::Marker(marker)) => {
                debug!("received marker in stream sink: {marker:?}");
                None
            }
            Err(e) => Some(Err(e)),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
