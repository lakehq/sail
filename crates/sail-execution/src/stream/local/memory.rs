use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::TryStreamExt;
use sail_common_datafusion::spillable_stream::{SpillableStream, SpillableStreamWriter};

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// Pipelined replicas share one backlog. Slow or unopened replicas retain their
/// cursor while the backlog spills, without blocking the other consumers.
pub(crate) struct MemoryStream {
    sender: Option<SpillableStreamWriter>,
    receivers: Vec<SendableRecordBatchStream>,
}

impl MemoryStream {
    pub fn new(
        context: &TaskContext,
        schema: SchemaRef,
        partition: usize,
        buffer: usize,
        replicas: usize,
    ) -> ExecutionResult<Self> {
        let (stream, sender) = SpillableStream::new(
            context,
            schema,
            &ExecutionPlanMetricsSet::new(),
            partition,
            buffer,
        );
        let receivers = (0..replicas)
            .map(|_| stream.subscribe())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            sender: Some(sender),
            receivers,
        })
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(MemoryStreamWriter(sender)))
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let stream = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(Box::pin(stream.map_err(|error| {
            TaskStreamError::External(Arc::new(error))
        })))
    }
}

struct MemoryStreamWriter(SpillableStreamWriter);

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        Ok(if self.0.write(batch).await? {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.0.finish();
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.0.finish();
        Ok(())
    }

    async fn fail(self: Box<Self>, error: Arc<DataFusionError>) -> Result<()> {
        self.0.fail(DataFusionError::Shared(error));
        Ok(())
    }
}
