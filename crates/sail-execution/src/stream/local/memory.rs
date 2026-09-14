use std::collections::VecDeque;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use log::debug;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    sender: Option<Box<dyn TaskStreamChannelSink>>,
    receivers: Vec<TaskStreamSource>,
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
            receivers.push(Box::pin(ReceiverStream::new(rx)) as TaskStreamSource);
        }
        let overflow = vec![VecDeque::new(); senders.len()];
        Self {
            sender: Some(Box::new(MemoryStreamReplicaSender { senders, overflow })),
            receivers,
        }
    }

    pub fn new_buffered(replicas: usize) -> Self {
        let mut senders = Vec::with_capacity(replicas);
        let mut receivers = Vec::with_capacity(replicas);
        for _ in 0..replicas {
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Box::pin(UnboundedReceiverStream::new(receiver)) as TaskStreamSource);
        }
        Self {
            sender: Some(Box::new(BufferedMemoryStreamReplicaSender { senders })),
            receivers,
        }
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(rx)
    }
}

struct BufferedMemoryStreamReplicaSender {
    senders: Vec<mpsc::UnboundedSender<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for BufferedMemoryStreamReplicaSender {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        self.senders
            .retain(|sender| sender.send(Ok(batch.clone())).is_ok());
        Ok(if self.senders.is_empty() {
            TaskStreamWriteState::Closed
        } else {
            TaskStreamWriteState::Active
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<mpsc::Sender<TaskStreamResult<RecordBatch>>>>,
    /// An overflow buffer for each sender to avoid blocking sending for slow senders.
    /// This also avoids deadlock situations where the task stream buffer size is small.
    // TODO: More investigation is needed to understand why deadlocks might happen among stages
    //   when the task stream buffer is of a limited size.
    overflow: Vec<VecDeque<TaskStreamResult<RecordBatch>>>,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let mut active = false;
        for (i, sender) in self.senders.iter_mut().enumerate() {
            if sender.is_none() {
                continue;
            }

            let overflow = &mut self.overflow[i];
            let mut dropped = false;

            if let Some(tx) = sender.as_ref() {
                // Try to flush overflow first
                while let Some(item) = overflow.pop_front() {
                    match tx.try_send(item) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_front(x);
                            break;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                            break;
                        }
                    }
                }
            }

            // A dropped receiver can happen under normal operation when the receiver no longer
            // needs more data (e.g., after a LIMIT operator has received enough rows).

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
                continue;
            }

            if let Some(tx) = sender.as_ref() {
                if overflow.is_empty() {
                    match tx.try_send(Ok(batch.clone())) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_back(x);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                        }
                    }
                } else {
                    overflow.push_back(Ok(batch.clone()));
                }
            }

            if dropped {
                debug!("memory stream replica receiver has been dropped");
                *sender = None;
                overflow.clear();
            } else {
                active = true;
            }
        }
        Ok(if active {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        for (i, sender) in self.senders.iter_mut().enumerate() {
            if sender.is_none() {
                continue;
            }

            let overflow = &mut self.overflow[i];
            let mut dropped = false;
            while let Some(item) = overflow.pop_front() {
                if let Some(tx) = sender.as_ref() {
                    // TODO: `send` here is blocking and may introduce deadlocks among tasks.
                    //   This is low-risk empirically though.
                    if tx.send(item).await.is_err() {
                        dropped = true;
                        break;
                    }
                }
            }

            if dropped {
                *sender = None;
                overflow.clear();
            }
        }
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;
    use futures::TryStreamExt;

    use super::MemoryStream;

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn buffered_stream_can_finish_before_a_consumer_subscribes() {
        let mut stream = MemoryStream::new_buffered(1);
        let mut sink = stream.publish().expect("buffered stream publisher");
        let schema = Arc::new(Schema::empty());
        for _ in 0..1_000 {
            sink.write(RecordBatch::new_empty(schema.clone()))
                .await
                .expect("buffered stream write");
        }
        sink.commit().await.expect("buffered stream commit");

        let batches = stream
            .subscribe()
            .expect("buffered stream subscriber")
            .try_collect::<Vec<_>>()
            .await
            .expect("buffered stream read");
        assert_eq!(batches.len(), 1_000);
    }
}
