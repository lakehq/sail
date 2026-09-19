use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::TryStreamExt;
use sail_common_datafusion::replay::{ReplayBuffer, ReplayOutput};
use tokio::sync::watch;

use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

pub struct ReplayStream {
    output: watch::Receiver<Option<Arc<ReplayOutput>>>,
}

impl ReplayStream {
    pub fn new(
        context: &TaskContext,
        schema: SchemaRef,
        partition: usize,
    ) -> (Self, Box<dyn TaskStreamChannelSink>) {
        let (tx, rx) = watch::channel(None);
        let writer = ReplayWriter {
            output: tx,
            buffer: ReplayBuffer::new(context, schema, &ExecutionPlanMetricsSet::new(), partition),
        };
        (Self { output: rx }, Box::new(writer))
    }

    pub fn subscribe(&self) -> TaskStreamSource {
        let mut output = self.output.clone();
        Box::pin(
            futures::stream::once(async move {
                let output = output
                    .wait_for(Option::is_some)
                    .await
                    .map_err(|_| {
                        TaskStreamError::Unknown("shared output was not committed".into())
                    })?
                    .clone()
                    .ok_or_else(|| TaskStreamError::Unknown("shared output is missing".into()))?;
                let stream = output
                    .stream()
                    .map_err(|error| TaskStreamError::External(Arc::new(error)))?;
                Ok::<_, TaskStreamError>(
                    stream.map_err(|error| TaskStreamError::External(Arc::new(error))),
                )
            })
            .try_flatten(),
        )
    }
}

struct ReplayWriter {
    output: watch::Sender<Option<Arc<ReplayOutput>>>,
    buffer: ReplayBuffer,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for ReplayWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        self.buffer.append(batch)?;
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.output
            .send_replace(Some(Arc::new(self.buffer.finish()?)));
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
