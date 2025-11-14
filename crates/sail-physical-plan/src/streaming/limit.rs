use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, Partitioning};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::{internal_err, plan_err, Result, Statistics};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::{
    FlowEventStreamAdapter, SendableFlowEventStream,
};
use sail_common_datafusion::streaming::event::FlowEvent;

/// A physical plan node that limits the number of retractable rows during
/// streaming query execution.
#[derive(Debug)]
pub struct StreamLimitExec {
    input: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
    data_schema: SchemaRef,
    properties: PlanProperties,
}

impl StreamLimitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let data_schema = Arc::new(try_from_flow_event_schema(&input.schema())?);
        let boundedness = if fetch.is_some() {
            Boundedness::Bounded
        } else {
            input.boundedness()
        };
        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            boundedness,
        );
        Ok(Self {
            input,
            data_schema,
            skip,
            fetch,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn skip(&self) -> usize {
        self.skip
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for StreamLimitExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamLimitExec {
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
        Ok(Arc::new(StreamLimitExec::try_new(
            child, self.skip, self.fetch,
        )?))
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
        let stream = DecodedFlowEventStream::try_new(stream)?;
        let state = StreamLimitState {
            stream: Some(Box::pin(stream)),
            skip: self.skip,
            fetch: self.fetch,
        };
        // The `.filter_map()` method creates a future for each batch.
        // The overhead should be acceptable since stream limit is mostly used for
        // testing, so the implementation does not need to be highly optimized.
        let stream =
            futures::stream::unfold(state, |state| state.step()).filter_map(|x| async move { x });
        let stream = Box::pin(FlowEventStreamAdapter::new(
            self.data_schema.clone(),
            stream,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(partition)?
            .with_fetch(self.fetch, self.skip, 1)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

struct StreamLimitState {
    stream: Option<SendableFlowEventStream>,
    skip: usize,
    fetch: Option<usize>,
}

impl StreamLimitState {
    fn skip_batch_if_needed(
        &mut self,
        batch: RecordBatch,
        retracted: BooleanArray,
    ) -> Option<(RecordBatch, BooleanArray)> {
        if self.skip == 0 {
            return Some((batch, retracted));
        }
        let length = batch.num_rows();
        if length <= self.skip {
            self.skip -= length;
            None
        } else {
            let batch = batch.slice(self.skip, length - self.skip);
            let retracted = retracted.slice(self.skip, length - self.skip);
            self.skip = 0;
            Some((batch, retracted))
        }
    }

    fn fetch_batch(
        &mut self,
        batch: RecordBatch,
        retracted: BooleanArray,
    ) -> Option<(RecordBatch, BooleanArray)> {
        if let Some(fetch) = self.fetch.as_mut() {
            if *fetch >= batch.num_rows() {
                *fetch -= batch.num_rows();
                Some((batch, retracted))
            } else if *fetch > 0 {
                let batch = batch.slice(0, *fetch);
                let retracted = retracted.slice(0, *fetch);
                self.fetch = Some(0);
                Some((batch, retracted))
            } else {
                None
            }
        } else {
            Some((batch, retracted))
        }
    }

    async fn step(self) -> Option<(Option<Result<FlowEvent>>, Self)> {
        let mut stream = self.stream?;
        let event = stream.next().await;
        let mut this = Self {
            stream: Some(stream),
            ..self
        };
        match event {
            Some(m @ Ok(FlowEvent::Marker(_))) => Some((Some(m), this)),
            Some(Ok(FlowEvent::Data { batch, retracted })) => {
                if let Some((batch, retracted)) = this.skip_batch_if_needed(batch, retracted) {
                    if let Some((batch, retracted)) = this.fetch_batch(batch, retracted) {
                        Some((Some(Ok(FlowEvent::Data { batch, retracted })), this))
                    } else {
                        // TODO: Here we close the stream when fetch limit is reached.
                        //   But we need to send the end-of-data marker and propagate
                        //   future markers until the next checkpoint has completed.
                        None
                    }
                } else {
                    Some((None, this))
                }
            }
            Some(Err(e)) => Some((Some(Err(e)), this)),
            None => None,
        }
    }
}
