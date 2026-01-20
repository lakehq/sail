use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use log::warn;
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
    sender: Option<MemoryStreamReplicaSender>,
    receivers: Vec<mpsc::Receiver<TaskStreamResult<RecordBatch>>>,
}

impl MemoryStream {
    pub fn new(
        buffer: usize,
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    ) -> Self {
        let replicas = replicas.max(senders.len());
        let diff = replicas - senders.len();
        let mut senders = senders.into_iter().map(Some).collect::<Vec<_>>();
        senders.reserve(diff);
        let mut receivers = Vec::with_capacity(diff);
        for _ in 0..diff {
            let (tx, rx) = mpsc::channel(buffer);
            senders.push(Some(tx));
            receivers.push(rx);
        }
        Self {
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
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<mpsc::Sender<TaskStreamResult<RecordBatch>>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> Result<()> {
        for sender in self.senders.iter_mut() {
            if let Some(s) = sender {
                if s.send(batch.clone()).await.is_err() {
                    warn!("memory stream replica receiver has been dropped");
                    *sender = None;
                }
            }
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
