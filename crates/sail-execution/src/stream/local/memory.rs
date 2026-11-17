use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use futures::Stream;
use futures::future::join_all;
use log::debug;
use tokio::sync::mpsc;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// An item in the channel of a memory stream replica.
#[derive(Clone)]
pub(crate) enum MemoryStreamItem {
    Batch(RecordBatch),
    /// The producer has committed the replica, so the replica is complete.
    End,
}

pub(crate) type MemoryStreamSender = mpsc::Sender<TaskStreamResult<MemoryStreamItem>>;

type MemoryStreamReceiver = mpsc::Receiver<TaskStreamResult<MemoryStreamItem>>;

/// Creates the channel of a memory stream replica.
pub(crate) fn memory_stream_channel(
    buffer: usize,
    key: &TaskStreamKey,
) -> (MemoryStreamSender, TaskStreamSource) {
    let (tx, rx) = mpsc::channel(buffer);
    (tx, MemoryStreamReader::source(key, rx))
}

/// Reads a memory stream replica. The replica ends only when the producer commits it.
/// If the channel is closed before that, the replica ends with an error instead.
struct MemoryStreamReader {
    key: TaskStreamKey,
    receiver: Option<MemoryStreamReceiver>,
}

impl MemoryStreamReader {
    fn source(key: &TaskStreamKey, receiver: MemoryStreamReceiver) -> TaskStreamSource {
        Box::pin(Self {
            key: key.clone(),
            receiver: Some(receiver),
        })
    }
}

impl Stream for MemoryStreamReader {
    type Item = TaskStreamResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        let last = match ready!(receiver.poll_recv(cx)) {
            Some(Ok(MemoryStreamItem::Batch(batch))) => return Poll::Ready(Some(Ok(batch))),
            Some(Ok(MemoryStreamItem::End)) => None,
            Some(Err(error)) => Some(Err(error)),
            None => Some(Err(TaskStreamError::Unknown(format!(
                "task stream for {} was closed before its producer finished",
                TaskStreamKeyDisplay(&self.key)
            )))),
        };
        self.receiver = None;
        Poll::Ready(last)
    }
}

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    sender: Option<MemoryStreamReplicaSender>,
    receivers: Vec<MemoryStreamReceiver>,
    /// A sender of each replica, kept for as long as the stream exists. If the producer
    /// goes away without committing, the readers wait until the stream is dropped.
    _open_senders: Vec<MemoryStreamSender>,
}

impl MemoryStream {
    pub fn new(buffer: usize, replicas: usize, senders: Vec<MemoryStreamSender>) -> Self {
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
        let overflow = vec![VecDeque::new(); senders.len()];
        let open_senders = senders.iter().flatten().cloned().collect();
        Self {
            sender: Some(MemoryStreamReplicaSender { senders, overflow }),
            receivers,
            _open_senders: open_senders,
        }
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(sender))
    }

    pub(crate) fn subscribe(&mut self, key: &TaskStreamKey) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(MemoryStreamReader::source(key, rx))
    }
}

struct MemoryStreamReplicaSender {
    senders: Vec<Option<MemoryStreamSender>>,
    /// An overflow buffer for each sender to avoid blocking sending for slow senders.
    /// This also avoids deadlock situations where the task stream buffer size is small.
    // TODO: More investigation is needed to understand deadlocks during writes when
    //   task stream buffers are bounded, and whether unbounded overflow can be avoided.
    //   Concurrent finalization fixes commit deadlocks but does not remove this need.
    overflow: Vec<VecDeque<TaskStreamResult<MemoryStreamItem>>>,
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
                let item = Ok(MemoryStreamItem::Batch(batch.clone()));
                if overflow.is_empty() {
                    match tx.try_send(item) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(x)) => {
                            overflow.push_back(x);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            dropped = true;
                        }
                    }
                } else {
                    overflow.push_back(item);
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

    async fn commit(self: Box<Self>) -> Result<()> {
        // Replicas can be consumed at different times or left unread entirely.
        // Flush them independently, ending each replica as soon as it is flushed
        // so that its consumer sees EOF without waiting for the other replicas.
        join_all(self.senders.into_iter().zip(self.overflow).map(
            |(sender, overflow)| async move {
                if let Some(tx) = sender {
                    for item in overflow.into_iter().chain([Ok(MemoryStreamItem::End)]) {
                        if tx.send(item).await.is_err() {
                            break;
                        }
                    }
                }
            },
        ))
        .await;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        // No replica is ended, so no reader takes the partial output for the whole stream.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array};
    use futures::{StreamExt, poll};

    use super::*;
    use crate::id::JobId;

    const CLOSED: &str = "task stream for job 1 stage 2 partition 3 attempt 4 channel 5 was closed before its producer finished";

    fn key() -> TaskStreamKey {
        TaskStreamKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
            channel: 5,
        }
    }

    fn batch(value: i32) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int32Array::from(vec![value])) as ArrayRef,
        )])?)
    }

    /// Reads the next batch that is immediately available.
    async fn value(reader: &mut TaskStreamSource) -> Option<i32> {
        match poll!(reader.next()) {
            Poll::Ready(Some(Ok(batch))) => batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|x| x.value(0)),
            _ => None,
        }
    }

    #[tokio::test]
    async fn commit_ends_all_replicas() -> ExecutionResult<()> {
        // One reader arrives before the stream is created, one while it is written,
        // and one after it has been committed.
        let (tx, mut early) = memory_stream_channel(4, &key());
        let mut stream = MemoryStream::new(4, 3, vec![tx]);
        let mut sink = stream.publish()?;
        let mut reader = stream.subscribe(&key())?;
        for i in 0..2 {
            assert_eq!(sink.write(batch(i)?).await?, TaskStreamWriteState::Active);
        }
        assert_eq!(value(&mut early).await, Some(0));
        assert_eq!(value(&mut reader).await, Some(0));
        sink.commit().await?;
        let mut late = stream.subscribe(&key())?;

        for reader in [&mut early, &mut reader] {
            assert_eq!(value(reader).await, Some(1));
            assert!(matches!(poll!(reader.next()), Poll::Ready(None)));
        }
        for i in 0..2 {
            assert_eq!(value(&mut late).await, Some(i));
        }
        assert!(matches!(poll!(late.next()), Poll::Ready(None)));

        // Removing the stream does not affect the readers that have reached the end.
        drop(stream);
        assert!(matches!(poll!(late.next()), Poll::Ready(None)));
        Ok(())
    }

    #[tokio::test]
    async fn uncommitted_stream_does_not_end() -> ExecutionResult<()> {
        for abort in [true, false] {
            let (tx, mut early) = memory_stream_channel(2, &key());
            let mut stream = MemoryStream::new(2, 2, vec![tx]);
            let mut sink = stream.publish()?;
            let mut reader = stream.subscribe(&key())?;
            assert_eq!(sink.write(batch(0)?).await?, TaskStreamWriteState::Active);
            if abort {
                sink.abort().await?;
            } else {
                drop(sink);
            }

            for reader in [&mut early, &mut reader] {
                assert_eq!(value(reader).await, Some(0));
                assert!(poll!(reader.next()).is_pending());
            }

            // Removing the stream ends the pending readers with an error.
            drop(stream);
            for reader in [&mut early, &mut reader] {
                assert!(matches!(
                    poll!(reader.next()),
                    Poll::Ready(Some(Err(error))) if error.to_string() == CLOSED
                ));
                assert!(matches!(poll!(reader.next()), Poll::Ready(None)));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_commit_ends_only_flushed_replicas() -> ExecutionResult<()> {
        let mut stream = MemoryStream::new(1, 2, vec![]);
        let mut sink = stream.publish()?;
        let mut slow = stream.subscribe(&key())?;
        let mut fast = stream.subscribe(&key())?;
        // The second batch of each replica does not fit into the channel.
        for i in 0..2 {
            assert_eq!(sink.write(batch(i)?).await?, TaskStreamWriteState::Active);
        }

        let mut commit = sink.commit();
        assert!(poll!(commit.as_mut()).is_pending());
        for i in 0..2 {
            assert_eq!(value(&mut fast).await, Some(i));
            assert!(poll!(commit.as_mut()).is_pending());
        }
        assert!(matches!(poll!(fast.next()), Poll::Ready(None)));

        // The producer is canceled while it waits for the other replica.
        drop(commit);
        assert_eq!(value(&mut slow).await, Some(0));
        assert!(poll!(slow.next()).is_pending());
        drop(stream);
        assert!(matches!(
            poll!(slow.next()),
            Poll::Ready(Some(Err(error))) if error.to_string() == CLOSED
        ));
        Ok(())
    }

    #[tokio::test]
    async fn commit_waits_for_room_in_full_replica() -> ExecutionResult<()> {
        for remove in [false, true] {
            let mut stream = MemoryStream::new(1, 1, vec![]);
            let mut sink = stream.publish()?;
            // The batch fills the channel, so there is no room for the end of the replica.
            assert_eq!(sink.write(batch(0)?).await?, TaskStreamWriteState::Active);
            let mut commit = sink.commit();
            assert!(poll!(commit.as_mut()).is_pending());

            if remove {
                drop(stream);
                assert!(matches!(poll!(commit.as_mut()), Poll::Ready(Ok(()))));
            } else {
                let mut reader = stream.subscribe(&key())?;
                assert_eq!(value(&mut reader).await, Some(0));
                assert!(matches!(poll!(commit.as_mut()), Poll::Ready(Ok(()))));
                assert!(matches!(poll!(reader.next()), Poll::Ready(None)));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn reader_hanging_up_during_commit_releases_producer() -> ExecutionResult<()> {
        let mut stream = MemoryStream::new(1, 1, vec![]);
        let mut sink = stream.publish()?;
        let reader = stream.subscribe(&key())?;
        for i in 0..2 {
            assert_eq!(sink.write(batch(i)?).await?, TaskStreamWriteState::Active);
        }
        let mut commit = sink.commit();
        assert!(poll!(commit.as_mut()).is_pending());
        drop(reader);
        assert!(matches!(poll!(commit.as_mut()), Poll::Ready(Ok(()))));
        Ok(())
    }

    #[tokio::test]
    async fn dropped_readers_close_sink() -> ExecutionResult<()> {
        let mut stream = MemoryStream::new(2, 2, vec![]);
        let mut sink = stream.publish()?;
        let first = stream.subscribe(&key())?;
        let second = stream.subscribe(&key())?;
        drop(first);
        assert_eq!(sink.write(batch(0)?).await?, TaskStreamWriteState::Active);
        drop(second);
        assert_eq!(sink.write(batch(1)?).await?, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn removed_pending_stream_fails_reader() {
        let (tx, mut reader) = memory_stream_channel(2, &key());
        assert!(poll!(reader.next()).is_pending());
        drop(tx);
        assert!(matches!(
            poll!(reader.next()),
            Poll::Ready(Some(Err(error))) if error.to_string() == CLOSED
        ));
    }
}
