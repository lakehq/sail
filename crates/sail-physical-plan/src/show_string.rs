use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{exec_err, internal_datafusion_err, DataFusionError, Result};
use futures::{Stream, StreamExt};
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::show_string::ShowStringFormat;

#[derive(Debug, Clone)]
pub struct ShowStringExec {
    input: Arc<dyn ExecutionPlan>,
    names: Vec<String>,
    limit: usize,
    format: ShowStringFormat,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl ShowStringExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        names: Vec<String>,
        limit: usize,
        format: ShowStringFormat,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            names,
            limit,
            format,
            schema,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn format(&self) -> &ShowStringFormat {
        &self.format
    }
}

impl DisplayAs for ShowStringExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ShowStringExec")
    }
}

impl ExecutionPlan for ShowStringExec {
    fn name(&self) -> &'static str {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .one()
            .map_err(|_| internal_datafusion_err!("ShowStringExec should have one child"))?;
        Ok(Arc::new(Self {
            input,
            ..self.as_ref().clone()
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("partition index out of range: {}", partition);
        }
        if self.input.output_partitioning().partition_count() != 1 {
            return exec_err!("ShowStringExec should have one input partition");
        }
        let input = rename_physical_plan(self.input.clone(), &self.names)?;
        let stream = input.execute(partition, context)?;
        Ok(Box::pin(ShowStringStream::new(
            stream,
            self.limit,
            self.format.clone(),
            self.schema.clone(),
        )))
    }
}

struct ShowStringStream {
    input: Option<SendableRecordBatchStream>,
    limit: usize,
    format: ShowStringFormat,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    data: Vec<RecordBatch>,
    has_more_data: bool,
}

enum ShowStringState {
    Continue,
    Show,
    Error(DataFusionError),
    Stopped,
}

impl ShowStringStream {
    pub fn new(
        input: SendableRecordBatchStream,
        limit: usize,
        format: ShowStringFormat,
        schema: SchemaRef,
    ) -> Self {
        let input_schema = input.schema();
        Self {
            input: Some(input),
            limit,
            format,
            input_schema,
            output_schema: schema,
            data: vec![],
            has_more_data: false,
        }
    }

    fn show(&self) -> Result<RecordBatch> {
        let batch = concat_batches(&self.input_schema, &self.data)?;
        let table = self.format.show(&batch, self.has_more_data)?;
        let array = StringArray::from(vec![table]);
        let batch = RecordBatch::try_new(self.output_schema.clone(), vec![Arc::new(array)])?;
        Ok(batch)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<ShowStringState> {
        let input = match self.input.as_mut() {
            Some(x) => x,
            None => return Poll::Ready(ShowStringState::Stopped),
        };
        let poll = input.poll_next_unpin(cx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(ShowStringState::Show),
            Poll::Ready(Some(Err(e))) => Poll::Ready(ShowStringState::Error(e)),
            Poll::Ready(Some(Ok(batch))) => match batch.num_rows() {
                n if n <= self.limit => {
                    self.data.push(batch);
                    self.limit -= n;
                    // We need to continue one more time when the limit reaches zero,
                    // since we need to know if there is more data.
                    Poll::Ready(ShowStringState::Continue)
                }
                _ => {
                    let batch = batch.slice(0, self.limit);
                    self.data.push(batch);
                    self.limit = 0;
                    self.has_more_data = true;
                    Poll::Ready(ShowStringState::Show)
                }
            },
        }
    }
}

impl Stream for ShowStringStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.poll(cx) {
                Poll::Pending => break Poll::Pending,
                Poll::Ready(ShowStringState::Show) => {
                    self.input = None;
                    break Poll::Ready(Some(self.show()));
                }
                Poll::Ready(ShowStringState::Error(e)) => {
                    self.input = None;
                    break Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(ShowStringState::Stopped) => {
                    break Poll::Ready(None);
                }
                Poll::Ready(ShowStringState::Continue) => continue,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.limit))
    }
}

impl RecordBatchStream for ShowStringStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
