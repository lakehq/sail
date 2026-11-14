use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use futures::{FutureExt, StreamExt};
use log::debug;
use tokio::sync::oneshot;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamResult;
use crate::stream::local::channel::{DistributionReceiver, DistributionSender, channels};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState};

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    /// The replicas that have not been read.
    receivers: Vec<DistributionReceiver<RecordBatch>>,
}

impl MemoryStream {
    /// Creates the memory streams for all channels of a task, along with the sink
    /// that writes to them. Each channel has at least the given number of replicas,
    /// and one replica for each reader that is already waiting for the channel.
    ///
    /// The sink waits when a replica of any channel holds more than `bound` batches
    /// that have not been read, unless a reader waits for data on a replica that has
    /// none. A reader that waits for data depends on the sink to continue, even if
    /// this means more batches for the readers that are behind.
    pub(crate) fn create(
        waiters: Vec<Vec<MemoryStreamWaiter>>,
        replicas: usize,
        bound: usize,
    ) -> (Box<dyn TaskStreamSink>, Vec<Self>) {
        let replicas = waiters
            .iter()
            .map(|waiters| replicas.max(waiters.len()))
            .collect::<Vec<_>>();
        let (senders, receivers) = channels(replicas.iter().sum(), bound);
        let mut senders = senders.into_iter().map(Some);
        let mut receivers = receivers.into_iter();
        let mut sink = Vec::with_capacity(waiters.len());
        let mut streams = Vec::with_capacity(waiters.len());
        for (waiters, replicas) in waiters.into_iter().zip(replicas) {
            sink.push(senders.by_ref().take(replicas).collect());
            let mut stream = Self {
                receivers: receivers.by_ref().take(replicas).collect(),
            };
            for waiter in waiters {
                if let Some(receiver) = stream.receivers.pop() {
                    // The replica is closed if the reader is no longer waiting.
                    let _ = waiter.send(Ok(receiver));
                }
            }
            streams.push(stream);
        }
        (Box::new(MemoryStreamSink { channels: sink }), streams)
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let receiver = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(source(receiver))
    }

    /// Returns a source for a reader that arrives before the memory stream is created,
    /// along with the handle to supply the replica for the reader later.
    /// The source ends without data if the handle is dropped.
    pub(crate) fn pending() -> (MemoryStreamWaiter, TaskStreamSource) {
        let (tx, rx) = oneshot::channel();
        let output = rx
            .map(|receiver| match receiver {
                Ok(Ok(receiver)) => source(receiver),
                Ok(Err(error)) => futures::stream::iter([Err(error)]).boxed(),
                Err(_) => futures::stream::empty().boxed(),
            })
            .flatten_stream();
        (tx, Box::pin(output))
    }
}

/// A handle to supply the replica for a reader that is waiting for a memory stream.
pub(crate) type MemoryStreamWaiter =
    oneshot::Sender<TaskStreamResult<DistributionReceiver<RecordBatch>>>;

fn source(receiver: DistributionReceiver<RecordBatch>) -> TaskStreamSource {
    Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move {
            let batch = receiver.recv().await?;
            Some((Ok(batch), receiver))
        },
    ))
}

/// A sink for all channels of a task. The data stays in memory until it is read,
/// and the readers see the end of the stream once the sink is committed or dropped.
/// A write waits for the readers as described in [`MemoryStream::create`].
struct MemoryStreamSink {
    /// The senders for the replicas of each channel.
    /// A sender is removed when the receiver of its replica has been dropped.
    channels: Vec<Vec<Option<DistributionSender<RecordBatch>>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MemoryStreamSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        if batches.len() != self.channels.len() {
            return Err(DataFusionError::Internal(format!(
                "expected {} shuffle output channels, got {}",
                self.channels.len(),
                batches.len()
            )));
        }
        for (senders, batch) in self.channels.iter_mut().zip(batches) {
            let Some(batch) = batch else {
                continue;
            };
            for sender in senders.iter_mut() {
                if let Some(tx) = sender
                    && tx.send(batch.clone()).await.is_err()
                {
                    // A dropped receiver can happen under normal operation when the receiver no longer
                    // needs more data (e.g., after a LIMIT operator has received enough rows).
                    debug!("memory stream replica receiver has been dropped");
                    *sender = None;
                }
            }
        }
        // The writer can stop when the readers of all channels are gone, including
        // the channels that it has not written to since their readers left.
        let mut senders = self.channels.iter().flatten().flatten();
        Ok(if senders.next().is_some_and(|tx| tx.has_receivers()) {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        // Replicas can be consumed at different times or left unread entirely.
        // Each reader sees the end of the stream after the data that remains for it,
        // so there is no need to wait for the readers here.
        self.channels.clear();
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

impl Drop for MemoryStreamSink {
    fn drop(&mut self) {
        // The senders that remain here have not been committed.
        // The data that has not been read is discarded since it is incomplete.
        for sender in self.channels.iter().flatten().flatten() {
            sender.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{AsArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use futures::TryStreamExt;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::stream::error::TaskStreamError;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    fn batch(value: i32) -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        let values = Int32Array::from(vec![value]);
        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(values)],
        )?)
    }

    async fn read(source: &mut TaskStreamSource) -> TaskStreamResult<Option<i32>> {
        let batch = source.try_next().await?;
        Ok(batch.map(|batch| batch.column(0).as_primitive::<Int32Type>().value(0)))
    }

    async fn read_all(mut source: TaskStreamSource) -> TaskStreamResult<Vec<i32>> {
        let mut values = vec![];
        while let Some(value) = read(&mut source).await? {
            values.push(value);
        }
        Ok(values)
    }

    type WriteTask = JoinHandle<Result<(Box<dyn TaskStreamSink>, TaskStreamWriteState)>>;

    /// Starts a write that has to wait.
    async fn start_waiting_write(
        mut sink: Box<dyn TaskStreamSink>,
        batches: Vec<Option<RecordBatch>>,
    ) -> WriteTask {
        let task = tokio::spawn(async move {
            let state = sink.write(batches).await?;
            Ok((sink, state))
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        task
    }

    /// Completes a write that no longer has to wait.
    async fn finish_write(
        task: WriteTask,
    ) -> std::result::Result<Box<dyn TaskStreamSink>, Box<dyn std::error::Error>> {
        let (sink, state) = tokio::time::timeout(Duration::from_secs(5), task).await???;
        assert_eq!(state, TaskStreamWriteState::Active);
        Ok(sink)
    }

    fn write_now(sink: &mut Box<dyn TaskStreamSink>, batches: Vec<Option<RecordBatch>>) {
        let state = sink.write(batches).now_or_never();
        assert!(matches!(state, Some(Ok(TaskStreamWriteState::Active))));
    }

    #[tokio::test]
    async fn write_waits_while_a_replica_is_over_the_bound() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 1, 1);
        let mut first = streams[0].subscribe()?;
        let mut second = streams[1].subscribe()?;
        write_now(&mut sink, vec![Some(batch(0)?), Some(batch(0)?)]);
        write_now(&mut sink, vec![Some(batch(1)?), None]);
        let task = start_waiting_write(sink, vec![None, Some(batch(1)?)]).await;

        // The write keeps waiting when a reader takes data from another channel.
        assert_eq!(read(&mut second).await?, Some(0));
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        // The write continues once the channel is within the bound again.
        assert_eq!(read(&mut first).await?, Some(0));
        let sink = finish_write(task).await?;
        sink.commit().await?;
        assert_eq!(read_all(first).await?, vec![1]);
        assert_eq!(read_all(second).await?, vec![1]);
        Ok(())
    }

    #[tokio::test]
    async fn write_does_not_wait_while_a_reader_waits_for_data() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 1, 0);
        let first = streams[0].subscribe()?;
        let mut second = streams[1].subscribe()?;
        assert!(futures::poll!(second.next()).is_pending());
        for i in 0..3 {
            write_now(&mut sink, vec![Some(batch(i)?), None]);
        }

        // The reader no longer waits once it has data.
        write_now(&mut sink, vec![None, Some(batch(0)?)]);
        let task = start_waiting_write(sink, vec![Some(batch(3)?), None]).await;
        assert_eq!(read(&mut second).await?, Some(0));
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        // The write continues once the reader waits for data again.
        assert!(futures::poll!(second.next()).is_pending());
        let sink = finish_write(task).await?;
        sink.commit().await?;
        assert_eq!(read_all(first).await?, vec![0, 1, 2, 3]);
        assert_eq!(read_all(second).await?, Vec::<i32>::new());
        Ok(())
    }

    #[tokio::test]
    async fn write_does_not_wait_for_readers_that_are_gone() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 1, 0);
        let reader = streams[0].subscribe()?;
        write_now(&mut sink, vec![Some(batch(0)?), None]);
        let task = start_waiting_write(sink, vec![None, Some(batch(0)?)]).await;
        drop(reader);
        let sink = finish_write(task).await?;

        // The replica that has not been read is removed.
        let task = start_waiting_write(sink, vec![Some(batch(1)?), Some(batch(1)?)]).await;
        streams.clear();
        let (_, state) = tokio::time::timeout(Duration::from_secs(5), task).await???;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn write_does_not_wait_for_replicas_that_are_never_read() -> TestResult {
        // Only one of the three replicas is read.
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![]], 3, 1);
        let reader = tokio::spawn(read_all(streams[0].subscribe()?));
        // The other replicas go over the bound, but the reader waits for data
        // whenever it has read what is there.
        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..20 {
                sink.write(vec![Some(batch(i)?)]).await?;
            }
            sink.commit().await
        })
        .await??;
        assert_eq!(reader.await??, (0..20).collect::<Vec<_>>());

        // The writer waits when the only reader has left, and stops when the
        // replicas that are never read are removed.
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![]], 3, 1);
        drop(streams[0].subscribe()?);
        write_now(&mut sink, vec![Some(batch(0)?)]);
        let task = start_waiting_write(sink, vec![Some(batch(1)?)]).await;
        streams.clear();
        let (_, state) = tokio::time::timeout(Duration::from_secs(5), task).await???;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn write_stops_when_the_readers_of_all_channels_are_gone() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 1, 1);
        let first = streams[0].subscribe()?;
        let second = streams[1].subscribe()?;

        // The writer only writes to the first channel.
        drop(second);
        let state = sink.write(vec![Some(batch(0)?), None]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        drop(first);
        let state = sink.write(vec![Some(batch(1)?), None]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn dropped_write_keeps_the_data_that_is_written() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 1, 0);
        let mut first = streams[0].subscribe()?;
        let second = streams[1].subscribe()?;
        {
            // The write has to wait after the batch for the first channel.
            let mut write = sink.write(vec![Some(batch(0)?), Some(batch(0)?)]);
            assert!(futures::poll!(&mut write).is_pending());
        }
        assert_eq!(read(&mut first).await?, Some(0));

        // The sink can be used for other writes afterwards.
        write_now(&mut sink, vec![None, Some(batch(1)?)]);
        sink.commit().await?;
        assert_eq!(read_all(first).await?, Vec::<i32>::new());
        assert_eq!(read_all(second).await?, vec![1]);
        Ok(())
    }

    #[tokio::test]
    async fn commit_and_abort_do_not_wait_for_readers_over_the_bound() -> TestResult {
        for commit in [true, false] {
            let (mut sink, mut streams) = MemoryStream::create(vec![vec![]], 1, 0);
            let reader = streams[0].subscribe()?;
            write_now(&mut sink, vec![Some(batch(0)?)]);
            let result = if commit {
                sink.commit().now_or_never()
            } else {
                sink.abort().now_or_never()
            };
            assert!(matches!(result, Some(Ok(()))));
            let expected = if commit { vec![0] } else { vec![] };
            assert_eq!(read_all(reader).await?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn commit_does_not_wait_for_readers() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 2, usize::MAX);
        for i in 0..100 {
            // The data stays in the stream when there is no reader.
            let state = sink
                .write(vec![Some(batch(i)?), Some(batch(-i)?)])
                .now_or_never();
            assert!(matches!(state, Some(Ok(TaskStreamWriteState::Active))));
        }
        assert!(matches!(sink.commit().now_or_never(), Some(Ok(()))));

        // Each replica of each channel has all the data of the channel in order.
        for (stream, sign) in streams.iter_mut().zip([1, -1]) {
            let expected = (0..100).map(|i| sign * i).collect::<Vec<_>>();
            assert_eq!(read_all(stream.subscribe()?).await?, expected);
            assert_eq!(read_all(stream.subscribe()?).await?, expected);
            assert!(stream.subscribe().is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn uncommitted_sink_discards_unread_data() -> TestResult {
        for abort in [true, false] {
            let (mut sink, mut streams) = MemoryStream::create(vec![vec![]], 2, usize::MAX);
            let mut reader = streams[0].subscribe()?;
            let first = batch(0)?;
            let data = Arc::downgrade(first.column(0));
            sink.write(vec![Some(first)]).await?;
            sink.write(vec![Some(batch(1)?)]).await?;
            assert_eq!(read(&mut reader).await?, Some(0));
            // The data remains for the replica that has not been read.
            assert_eq!(data.strong_count(), 1);

            if abort {
                sink.abort().await?;
            } else {
                drop(sink);
            }
            assert_eq!(data.strong_count(), 0);
            // The readers see the end of the stream, whether or not they have started.
            assert_eq!(read(&mut reader).await?, None);
            assert_eq!(read_all(streams[0].subscribe()?).await?, Vec::<i32>::new());
        }
        Ok(())
    }

    #[tokio::test]
    async fn closes_after_all_readers_are_gone() -> TestResult {
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![], vec![]], 2, usize::MAX);
        let mut reader = streams[0].subscribe()?;
        drop(streams[0].subscribe()?);

        // A dropped reader does not affect the other replica of the channel.
        let first = batch(0)?;
        let data = Arc::downgrade(first.column(0));
        let state = sink.write(vec![Some(first), None]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        assert_eq!(read(&mut reader).await?, Some(0));
        assert_eq!(data.strong_count(), 0);

        // The other channel still has replicas that may be read later.
        drop(reader);
        let state = sink.write(vec![Some(batch(1)?), Some(batch(1)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);

        streams.clear();
        let state = sink.write(vec![None, Some(batch(2)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn supplies_replicas_to_waiting_readers() -> TestResult {
        let (waiter, mut reader) = MemoryStream::pending();
        let (gone, _) = MemoryStream::pending();
        assert!(futures::poll!(reader.next()).is_pending());

        // The channel has one replica for each reader that arrived before its creation.
        let (mut sink, mut streams) = MemoryStream::create(vec![vec![waiter, gone]], 1, usize::MAX);
        assert!(streams[0].subscribe().is_err());

        let state = sink.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        assert_eq!(read(&mut reader).await?, Some(0));

        // The replica of the reader that left before the creation is closed as well.
        drop(reader);
        let state = sink.write(vec![Some(batch(1)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    }

    #[tokio::test]
    async fn ends_waiting_readers_without_replica() -> TestResult {
        let (waiter, mut reader) = MemoryStream::pending();
        let error = TaskStreamError::Unknown("failed".to_string());
        assert!(waiter.send(Err(error)).is_ok());
        assert!(read(&mut reader).await.is_err());
        assert_eq!(read(&mut reader).await?, None);

        let (waiter, mut reader) = MemoryStream::pending();
        drop(waiter);
        assert_eq!(read(&mut reader).await?, None);
        Ok(())
    }
}
