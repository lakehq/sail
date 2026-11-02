use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryStreamExt, stream};
use sail_common_datafusion::replay::{ReplayPartition, ReplayWriter};
use tokio::sync::oneshot;

use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

type Committed = Shared<BoxFuture<'static, Result<Arc<ReplayPartition>, TaskStreamError>>>;

pub struct SpillStream {
    committed: Committed,
    context: Arc<TaskContext>,
}

impl SpillStream {
    pub fn try_new(
        schema: SchemaRef,
        context: Arc<TaskContext>,
    ) -> Result<(Self, Box<dyn TaskStreamChannelSink>)> {
        let writer = ReplayWriter::try_new(&context, schema)?;
        let (tx, rx) = oneshot::channel();
        let committed = async move {
            rx.await
                .map_err(|_| TaskStreamError::Unknown("materialized partition was aborted".into()))
        }
        .boxed()
        .shared();
        let stream = Self { committed, context };
        Ok((
            stream,
            Box::new(SpillSink {
                writer,
                committed: tx,
            }),
        ))
    }

    pub fn subscribe(&self) -> TaskStreamSource {
        let committed = self.committed.clone();
        let context = self.context.clone();
        Box::pin(
            stream::once(async move {
                let replay = committed.await?;
                replay
                    .open(&context)
                    .map(|stream| {
                        stream.map_err(|error| TaskStreamError::External(Arc::new(error)))
                    })
                    .map_err(|error| TaskStreamError::External(Arc::new(error)))
            })
            .try_flatten(),
        )
    }
}

struct SpillSink {
    writer: ReplayWriter,
    committed: oneshot::Sender<Arc<ReplayPartition>>,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for SpillSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        if self.committed.is_closed() {
            return Ok(TaskStreamWriteState::Closed);
        }
        self.writer.append(&batch)?;
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let Self { writer, committed } = *self;
        if committed.send(Arc::new(writer.finish()?)).is_err() {
            return exec_err!("materialized partition owner was removed before commit");
        }
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        // Dropping the writer removes the incomplete file and closes all waiting readers.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn publish_commit_abort_and_independent_readers() -> Result<()> {
        let context = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let (stream, mut sink) = SpillStream::try_new(schema.clone(), context.clone())?;
        sink.write(batch.clone()).await?;
        let mut reader = stream.subscribe();
        assert!(futures::poll!(reader.next()).is_pending());
        sink.commit().await?;
        let second = stream.subscribe();
        drop(stream);
        let first = reader
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let second = second
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        assert_eq!(first, vec![batch.clone()]);
        assert_eq!(first, second);
        assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
        let (stream, mut sink) = SpillStream::try_new(schema.clone(), context.clone())?;
        sink.write(batch).await?;
        sink.abort().await?;
        assert!(stream.subscribe().next().await.is_some_and(|r| r.is_err()));
        assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
        let (stream, sink) = SpillStream::try_new(schema, context.clone())?;
        sink.commit().await?;
        assert!(stream.subscribe().next().await.is_none());
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }
}
