use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamSink;

pub trait LocalStream: Send {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>>;
    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource>;
}

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// The slowest receiver controls the rate of the sender,
/// and the overall memory usage is bounded.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    replicas: usize,
    sender: Option<MemoryStreamReplicaSender>,
    receivers: Vec<mpsc::Receiver<TaskStreamResult<RecordBatch>>>,
}

impl MemoryStream {
    pub fn new(buffer: usize, replicas: usize) -> Self {
        let mut senders = Vec::with_capacity(replicas);
        let mut receivers = Vec::with_capacity(replicas);
        for _ in 0..replicas {
            let (tx, rx) = mpsc::channel(buffer);
            senders.push(tx);
            receivers.push(rx);
        }
        Self {
            replicas,
            sender: Some(MemoryStreamReplicaSender { senders }),
            receivers,
        }
    }
}

impl LocalStream for MemoryStream {
    fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(sender))
    }

    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError(format!(
                "memory stream has exhausted all {} replica(s)",
                self.replicas
            ))
        })?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> Result<()> {
        for tx in self.senders.iter_mut() {
            tx.send(batch.clone())
                .await
                .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
