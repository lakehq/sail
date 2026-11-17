use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use futures::{FutureExt, StreamExt};
use log::warn;
use sail_common_datafusion::array::record_batch::cast_record_batch_positionally;
use tokio::sync::oneshot;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::stream::error::TaskStreamResult;
use crate::stream::local::queue::{
    QueueLimit, QueueReader, QueueWrite, QueueWriter, STALL_TIMEOUT, StreamMemory, task_queues,
};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState};

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
/// Since [`Arc`] is used inside the record batch, it is relatively cheap
/// to clone the data in multiple replicas.
pub(crate) struct MemoryStream {
    /// The replicas that have not been read.
    receivers: Vec<QueueReader<RecordBatch>>,
}

impl MemoryStream {
    /// Creates the memory streams for all channels of a task, along with the sink
    /// that writes to them. Each channel has at least the given number of replicas,
    /// and one replica for each reader that is already waiting for the channel.
    ///
    /// With a limit, the sink waits for the readers while the streams in the given
    /// memory of the process have reached the ceiling of the limit, and the replicas
    /// of all channels together have at least the rows of the limit. The sink does
    /// not continue because a reader waits for data, so each replica needs a reader
    /// that takes the data as it comes. A batch counts for the memory until it is
    /// taken from all replicas of its channel. Its size is the memory that its
    /// arrays refer to, so the batches are expected not to share their memory with
    /// other data, which holds for the batches of a shuffle up to a factor of two:
    /// a batch that is passed on whole may keep up to twice the memory that its
    /// rows need.
    ///
    /// The sink gives the batches the schema of the streams, as described in
    /// [`with_stream_schema`].
    pub(crate) fn create(
        key: &TaskKey,
        waiters: Vec<Vec<MemoryStreamWaiter>>,
        replicas: usize,
        limit: Option<QueueLimit>,
        memory: Arc<StreamMemory>,
        schema: SchemaRef,
    ) -> (Box<dyn TaskStreamSink>, Vec<Self>) {
        let replicas = waiters
            .iter()
            .map(|waiters| replicas.max(waiters.len()))
            .collect::<Vec<_>>();
        let (writer, receivers) = task_queues(
            &replicas,
            limit,
            memory,
            RecordBatch::num_rows,
            RecordBatch::get_array_memory_size,
        );
        let mut receivers = receivers.into_iter();
        let mut streams = Vec::with_capacity(waiters.len());
        for (waiters, replicas) in waiters.into_iter().zip(&replicas) {
            let mut stream = Self {
                receivers: receivers.by_ref().take(*replicas).collect(),
            };
            for waiter in waiters {
                if let Some(receiver) = stream.receivers.pop() {
                    // The replica is closed if the reader is no longer waiting.
                    let _ = waiter.send(Ok(receiver));
                }
            }
            streams.push(stream);
        }
        let sink = MemoryStreamSink {
            key: key.clone(),
            writer,
            channels: replicas.len(),
            schema,
        };
        (Box::new(sink), streams)
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
pub(crate) type MemoryStreamWaiter = oneshot::Sender<TaskStreamResult<QueueReader<RecordBatch>>>;

fn source(receiver: QueueReader<RecordBatch>) -> TaskStreamSource {
    Box::pin(receiver.map(TaskStreamResult::Ok))
}

/// Returns the batch with the schema of the stream that it is written to.
///
/// The batches of a task can differ from the schema of its stage in the names and
/// the nullability of the columns, since some operators pass on the batches of their
/// inputs as they are. In particular, the tasks of a union can run different inputs.
/// The readers need one schema for all the tasks of a stage, since the batches of
/// several tasks are encoded together for a reader on another worker, with the
/// schema of the first batch. The columns are taken by position, as a reader on
/// another worker does for the batches that it receives.
///
/// A batch that does not fit the schema of the stream is passed on as it is, so that
/// it reaches a reader on the same worker like any other batch.
fn with_stream_schema(batch: RecordBatch, schema: &SchemaRef) -> RecordBatch {
    if batch.schema_ref() == schema {
        return batch;
    }
    cast_record_batch_positionally(batch.clone(), schema.clone()).unwrap_or(batch)
}

/// A sink for all channels of a task. The data stays in memory until it is read,
/// and each reader sees the end of the stream after the data that remains for it,
/// once the sink is committed or dropped, whichever way the task has ended.
/// A write waits for the readers as described in [`MemoryStream::create`].
struct MemoryStreamSink {
    key: TaskKey,
    writer: QueueWriter<RecordBatch>,
    channels: usize,
    /// The schema of the streams.
    schema: SchemaRef,
}

#[tonic::async_trait]
impl TaskStreamSink for MemoryStreamSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        if batches.len() != self.channels {
            return Err(DataFusionError::Internal(format!(
                "expected {} shuffle output channels, got {}",
                self.channels,
                batches.len()
            )));
        }
        // A reader that is gone can happen under normal operation when it no longer
        // needs more data (e.g., after a LIMIT operator has received enough rows).
        // The writer can stop when the readers of all channels are gone, including
        // the channels that it has not written to since their readers left.
        let batches = batches
            .into_iter()
            .map(|batch| batch.map(|batch| with_stream_schema(batch, &self.schema)))
            .collect();
        match self.writer.write(batches).await {
            QueueWrite::Queued => Ok(TaskStreamWriteState::Active),
            QueueWrite::QueuedAfterStall => {
                warn!(
                    "task stream of {} stopped waiting for its readers after {} seconds without progress",
                    TaskKeyDisplay(&self.key),
                    STALL_TIMEOUT.as_secs()
                );
                Ok(TaskStreamWriteState::Active)
            }
            QueueWrite::Closed => Ok(TaskStreamWriteState::Closed),
        }
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // Replicas can be consumed at different times or left unread entirely.
        // Each reader sees the end of the stream after the data that remains for it,
        // so there is no need to wait for the readers here.
        let Self { writer, .. } = *self;
        writer.finish();
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::arrow::array::{AsArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use futures::TryStreamExt;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::id::JobId;
    use crate::stream::error::TaskStreamError;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    /// Creates the streams of a task in a process that has no memory left for
    /// them, so that the sink waits whenever the streams have the given number
    /// of rows.
    fn create(
        waiters: Vec<Vec<MemoryStreamWaiter>>,
        replicas: usize,
        rows: Option<usize>,
    ) -> (Box<dyn TaskStreamSink>, Vec<MemoryStream>) {
        let limit = rows.map(|rows| QueueLimit { rows, ceiling: 1 });
        create_with_memory(waiters, replicas, limit, Arc::default())
    }

    fn create_with_memory(
        waiters: Vec<Vec<MemoryStreamWaiter>>,
        replicas: usize,
        limit: Option<QueueLimit>,
        memory: Arc<StreamMemory>,
    ) -> (Box<dyn TaskStreamSink>, Vec<MemoryStream>) {
        let key = TaskKey {
            job_id: JobId::from(1),
            stage: 0,
            partition: 0,
            attempt: 0,
        };
        MemoryStream::create(&key, waiters, replicas, limit, memory, schema())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]))
    }

    /// Creates a batch with the given number of rows that all have the given value.
    fn rows(value: i32, rows: usize) -> Result<RecordBatch> {
        let values = Int32Array::from(vec![value; rows]);
        Ok(RecordBatch::try_new(schema(), vec![Arc::new(values)])?)
    }

    fn batch(value: i32) -> Result<RecordBatch> {
        rows(value, 1)
    }

    #[test]
    fn batch_takes_the_schema_of_the_stream_if_it_fits() -> TestResult {
        let batch = |name: &str, values: Vec<Option<i32>>| {
            let schema = Schema::new(vec![Field::new(name, DataType::Int32, true)]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(values))])
        };
        // A column with another name and without nulls fits a schema without nulls.
        let output = with_stream_schema(batch("other", vec![Some(1)])?, &schema());
        assert_eq!(output.schema(), schema());
        assert_eq!(output.column(0).as_primitive::<Int32Type>().value(0), 1);
        // A column with nulls does not, and the batch stays as it is.
        let nulls = batch("value", vec![None])?;
        assert_eq!(with_stream_schema(nulls.clone(), &schema()), nulls);
        Ok(())
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

    #[tokio::test(start_paused = true)]
    async fn write_stops_waiting_when_the_readers_make_no_progress() -> TestResult {
        let (mut sink, mut streams) = create(vec![vec![], vec![]], 1, Some(1));
        let first = streams[0].subscribe()?;
        let mut second = streams[1].subscribe()?;
        // One reader waits for data, while the other one does not take its data.
        assert!(futures::poll!(second.next()).is_pending());
        write_now(&mut sink, vec![Some(batch(0)?), None]);
        let start = tokio::time::Instant::now();
        let state = sink.write(vec![Some(batch(1)?), None]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        assert!(start.elapsed() >= STALL_TIMEOUT);

        // The sink never waits again, so that the reader gets its data eventually.
        for i in 2..10 {
            write_now(&mut sink, vec![Some(batch(i)?), None]);
        }
        write_now(&mut sink, vec![None, Some(batch(0)?)]);
        assert_eq!(read(&mut second).await?, Some(0));
        sink.commit().await?;
        assert_eq!(read_all(first).await?, (0..10).collect::<Vec<_>>());
        Ok(())
    }

    #[tokio::test]
    async fn commit_and_abort_do_not_wait_for_readers() -> TestResult {
        for commit in [true, false] {
            let (mut sink, mut streams) = create(vec![vec![]], 1, Some(0));
            let reader = streams[0].subscribe()?;
            write_now(&mut sink, vec![Some(batch(0)?)]);
            let result = if commit {
                sink.commit().now_or_never()
            } else {
                sink.abort().now_or_never()
            };
            assert!(matches!(result, Some(Ok(()))));
            assert_eq!(read_all(reader).await?, vec![0]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn commit_keeps_the_data_for_readers_that_come_later() -> TestResult {
        let (mut sink, mut streams) = create(vec![vec![], vec![]], 2, None);
        for i in 0..100 {
            // The data stays in the stream when there is no reader.
            write_now(&mut sink, vec![Some(batch(i)?), Some(batch(-i)?)]);
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
    async fn uncommitted_sink_leaves_the_data_to_the_readers() -> TestResult {
        for abort in [true, false] {
            let (mut sink, mut streams) = create(vec![vec![]], 2, None);
            let mut reader = streams[0].subscribe()?;
            let first = batch(0)?;
            let data = Arc::downgrade(first.column(0));
            sink.write(vec![Some(first)]).await?;
            sink.write(vec![Some(batch(1)?)]).await?;
            assert_eq!(read(&mut reader).await?, Some(0));

            if abort {
                sink.abort().await?;
            } else {
                drop(sink);
            }
            // The readers see the end of the stream after the data that was written,
            // whether or not they have started.
            assert_eq!(read_all(reader).await?, vec![1]);
            assert_eq!(data.strong_count(), 1);
            assert_eq!(read_all(streams[0].subscribe()?).await?, vec![0, 1]);
            assert_eq!(data.strong_count(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn write_waits_only_when_the_process_is_short_of_memory() -> TestResult {
        // The streams of the process can hold three batches and a bit.
        let rows = |value| rows(value, 65536);
        let size = rows(0)?.get_array_memory_size();
        assert!(size >= 65536 * 4);
        let memory = Arc::new(StreamMemory::default());
        let limit = QueueLimit {
            rows: 65536,
            ceiling: 3 * size + size / 2,
        };
        let (mut sink, mut streams) =
            create_with_memory(vec![vec![]], 1, Some(limit), memory.clone());
        let mut reader = streams[0].subscribe()?;
        for i in 0..4 {
            write_now(&mut sink, vec![Some(rows(i)?)]);
        }
        assert_eq!(memory.used(), 4 * size);
        let task = start_waiting_write(sink, vec![Some(rows(4)?)]).await;
        assert_eq!(read(&mut reader).await?, Some(0));
        let sink = finish_write(task).await?;
        assert_eq!(memory.used(), 4 * size);
        sink.commit().await?;
        assert_eq!(read_all(reader).await?, vec![1, 2, 3, 4]);
        assert_eq!(memory.used(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn closes_after_all_readers_are_gone() -> TestResult {
        let (mut sink, mut streams) = create(vec![vec![], vec![]], 2, None);
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
        let (mut sink, mut streams) = create(vec![vec![waiter, gone]], 1, None);
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
    async fn does_not_wait_for_readers_that_left_before_the_creation() -> TestResult {
        // The rows for a reader that is gone do not count, so the sink only
        // waits for the reader that is there.
        let (waiter, mut reader) = MemoryStream::pending();
        let (gone, _) = MemoryStream::pending();
        let (mut sink, _streams) = create(vec![vec![waiter], vec![gone]], 1, Some(1));
        write_now(&mut sink, vec![Some(batch(0)?), Some(rows(0, 100)?)]);
        write_now(&mut sink, vec![None, Some(rows(1, 100)?)]);
        let task = start_waiting_write(sink, vec![Some(batch(1)?), None]).await;
        assert_eq!(read(&mut reader).await?, Some(0));
        let sink = finish_write(task).await?;
        sink.commit().await?;
        assert_eq!(read_all(reader).await?, vec![1]);
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
