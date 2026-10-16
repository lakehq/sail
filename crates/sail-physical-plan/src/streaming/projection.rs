use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions, new_null_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, plan_err};
use futures::TryStreamExt;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;

/// Preserve event order and avoid evaluating data expressions on marker placeholders.
#[derive(Debug)]
pub struct StreamProjectionExec {
    projection: Arc<ProjectionExec>,
}

impl StreamProjectionExec {
    pub fn try_new(projection: Arc<ProjectionExec>) -> Result<Self> {
        try_from_flow_event_schema(&projection.input().schema())?;
        try_from_flow_event_schema(&projection.schema())?;
        Ok(Self { projection })
    }
    pub fn projection(&self) -> &Arc<ProjectionExec> {
        &self.projection
    }
}

impl DisplayAs for StreamProjectionExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "Stream")?;
        self.projection.fmt_as(t, f)
    }
}

impl ExecutionPlan for StreamProjectionExec {
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.projection.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.projection.input()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.projection.apply_expressions(f)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (Some(input), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("StreamProjectionExec requires one input");
        };
        Ok(Arc::new(Self::try_new(Arc::new(
            ProjectionExec::try_new_with_schema_metadata(
                self.projection.expr().to_vec(),
                input,
                &self.projection.schema(),
            )?,
        ))?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.projection.input();
        let flow_schema = input.schema();
        let schema = Arc::new(try_from_flow_event_schema(&self.schema())?);
        let output_schema = schema.clone();
        let projector = self
            .projection
            .projection_expr()
            .make_projector_with_schema_metadata(&flow_schema, &self.schema())?;
        let stream = DecodedFlowEventStream::try_new(input.execute(partition, context)?)?.and_then(
            move |event| {
                let result = (|| match event {
                    FlowEvent::Marker(marker) => Ok(FlowEvent::Marker(marker)),
                    FlowEvent::Data { batch, retracted } => {
                        let count = batch.num_rows();
                        let mut columns = vec![
                            new_null_array(&DataType::Binary, count),
                            Arc::new(retracted.clone()) as _,
                        ];
                        columns.extend_from_slice(batch.columns());
                        let batch = RecordBatch::try_new(flow_schema.clone(), columns)?;
                        let batch = projector.project_batch(&batch)?;
                        let data = RecordBatch::try_new_with_options(
                            schema.clone(),
                            batch.columns()[2..].to_vec(),
                            &RecordBatchOptions::new().with_row_count(Some(count)),
                        )?;
                        Ok(FlowEvent::Data {
                            batch: data,
                            retracted,
                        })
                    }
                })();
                futures::future::ready(result)
            },
        );
        Ok(Box::pin(EncodedFlowEventStream::new(Box::pin(
            FlowEventStreamAdapter::new(output_schema, stream),
        ))))
    }
}
