use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryLimit;
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use futures::StreamExt;
use futures::future::join_all;
use log::debug;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// A memory stream that can be read multiple times.
/// It maintains multiple replicas of the stream internally.
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
        Self::create(buffer, replicas, senders, false, None)
    }

    /// Creates the stream of a shared stage. The task that writes the stream ends it with
    /// its error instead of failing. If the memory pool is limited and temporary files are
    /// enabled, the batches that a replica has no room for are kept on disk.
    pub fn new_shared(
        buffer: usize,
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        context: &TaskContext,
        schema: SchemaRef,
    ) -> Self {
        let spill = (matches!(context.memory_pool().memory_limit(), MemoryLimit::Finite(_))
            && context.runtime_env().disk_manager.tmp_files_enabled())
        .then(|| {
            let spill = SpillManager::new(
                context.runtime_env(),
                SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                schema,
            )
            .with_compression_type(context.session_config().spill_compression());
            let file_size = context
                .session_config()
                .options()
                .execution
                .max_spill_file_size_bytes
                .get();
            (Arc::new(spill), file_size)
        });
        Self::create(buffer, replicas, senders, true, spill)
    }

    /// Creates the stream, with the spill manager and the spill file size for an overflow
    /// that is kept on disk.
    fn create(
        buffer: usize,
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        shared: bool,
        spill: Option<(Arc<SpillManager>, usize)>,
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
        let replicas = senders
            .into_iter()
            .map(|tx| Replica {
                tx,
                overflow: match &spill {
                    Some((spill, file_size)) => Overflow::Disk(DiskOverflow {
                        spill: Arc::clone(spill),
                        file_size: *file_size,
                        files: VecDeque::new(),
                        reading: None,
                        writing: None,
                        len: 0,
                    }),
                    None => Overflow::Memory(VecDeque::new()),
                },
            })
            .collect();
        Self {
            sender: Some(MemoryStreamReplicaSender { replicas, shared }),
            receivers,
        }
    }

    pub(crate) fn publish(&mut self) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".to_string())
        })?;
        Ok(Box::new(sender))
    }

    pub(crate) fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        let rx = self.receivers.pop().ok_or_else(|| {
            ExecutionError::InternalError("memory stream has exhausted all replica(s)".to_string())
        })?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

struct MemoryStreamReplicaSender {
    replicas: Vec<Replica>,
    /// Whether the stream is the output of a shared stage, whose error is the last item
    /// of every replica, so that it fails only the readers that read up to it.
    shared: bool,
}

/// A replica of the stream that is written.
/// A replica that is dropped before it is finished ends with an error, so that its
/// reader does not take the batches that are written so far for the whole stream.
struct Replica {
    /// The sender, which is [None] once the replica is finished or its receiver is dropped.
    tx: Option<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    overflow: Overflow,
}

/// An overflow buffer for the sender to avoid blocking sending for slow senders.
/// This also avoids deadlock situations where the task stream buffer size is small.
// TODO: More investigation is needed to understand deadlocks during writes when
//   task stream buffers are bounded, and whether unbounded overflow can be avoided.
//   Concurrent finalization fixes commit deadlocks but does not remove this need.
enum Overflow {
    Memory(VecDeque<TaskStreamResult<RecordBatch>>),
    /// The overflow of a shared stage when the memory pool is limited, which is kept
    /// on disk and takes no memory from the pool.
    Disk(DiskOverflow),
}

impl Overflow {
    fn clear(&mut self) {
        match self {
            Overflow::Memory(overflow) => overflow.clear(),
            Overflow::Disk(overflow) => {
                overflow.files.clear();
                overflow.reading = None;
                overflow.writing = None;
                overflow.len = 0;
            }
        }
    }
}

/// Appends a batch to a spill file, or finishes the file and returns it.
type SpillWriter =
    Box<dyn FnMut(Option<&RecordBatch>) -> Result<Option<Arc<dyn SpillFile>>> + Send>;

/// Batches in spill files, which are read in the order they are written.
/// A spill file is read once it is finished, and is deleted once it is read.
struct DiskOverflow {
    spill: Arc<SpillManager>,
    file_size: usize,
    /// The spill files that are finished and not yet read.
    files: VecDeque<Arc<dyn SpillFile>>,
    /// The spill file that is read.
    reading: Option<SendableRecordBatchStream>,
    /// The spill file that is written, and the size of the batches written to it.
    writing: Option<(SpillWriter, usize)>,
    /// The number of batches in the spill files.
    len: usize,
}

impl DiskOverflow {
    fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        if self
            .writing
            .as_ref()
            .is_some_and(|(_, size)| *size >= self.file_size)
        {
            self.finish_writing()?;
        }
        let (writer, size) = match &mut self.writing {
            Some(writing) => writing,
            None => {
                let mut file = self.spill.create_in_progress_file("MemoryStream")?;
                let writer: SpillWriter = Box::new(move |batch| match batch {
                    Some(batch) => {
                        file.append_batch(batch)?;
                        Ok(None)
                    }
                    None => file.finish(),
                });
                self.writing.insert((writer, 0))
            }
        };
        writer(Some(batch))?;
        *size += batch.get_array_memory_size();
        self.len += 1;
        Ok(())
    }

    fn finish_writing(&mut self) -> Result<()> {
        if let Some((mut writer, _)) = self.writing.take()
            && let Some(file) = writer(None)?
        {
            self.files.push_back(file);
        }
        Ok(())
    }

    async fn pop(&mut self) -> Result<RecordBatch> {
        loop {
            if let Some(reading) = &mut self.reading {
                if let Some(batch) = reading.next().await {
                    self.len = self.len.saturating_sub(1);
                    return batch;
                }
                self.reading = None;
            }
            if self.files.is_empty() {
                // The file that is written is finished, so that it can be read.
                self.finish_writing()?;
            }
            let Some(file) = self.files.pop_front() else {
                return Err(DataFusionError::Internal(
                    "missing batch in the overflow of a memory stream".to_string(),
                ));
            };
            self.reading = Some(self.spill.read_spill_as_stream_unbuffered(file, None)?);
        }
    }

    /// Writes the batch to the replica, after the overflow, and returns whether the
    /// receiver of the replica has been dropped.
    async fn write(
        &mut self,
        tx: &mpsc::Sender<TaskStreamResult<RecordBatch>>,
        batch: &RecordBatch,
    ) -> Result<bool> {
        while self.len > 0 {
            match tx.try_reserve() {
                Ok(permit) => permit.send(Ok(self.pop().await?)),
                Err(mpsc::error::TrySendError::Full(())) => break,
                Err(mpsc::error::TrySendError::Closed(())) => return Ok(true),
            }
        }
        if self.len == 0 {
            match tx.try_send(Ok(batch.clone())) {
                Ok(()) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(true),
            }
        }
        self.push(batch)?;
        Ok(false)
    }
}

/// Writes the batch to the replica, after the overflow, and returns whether the
/// receiver of the replica has been dropped.
fn write_with_memory_overflow(
    tx: &mpsc::Sender<TaskStreamResult<RecordBatch>>,
    overflow: &mut VecDeque<TaskStreamResult<RecordBatch>>,
    batch: &RecordBatch,
) -> bool {
    // Try to flush overflow first
    while let Some(item) = overflow.pop_front() {
        match tx.try_send(item) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(x)) => {
                overflow.push_front(x);
                break;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return true;
            }
        }
    }

    if overflow.is_empty() {
        match tx.try_send(Ok(batch.clone())) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(x)) => {
                overflow.push_back(x);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return true;
            }
        }
    } else {
        overflow.push_back(Ok(batch.clone()));
    }
    false
}

impl Replica {
    /// Sends the overflow and then the error if any, and finishes the replica.
    async fn finish(&mut self, error: Option<TaskStreamError>) {
        if let Some(tx) = &self.tx {
            let flushed = match &mut self.overflow {
                Overflow::Memory(overflow) => {
                    let mut flushed = true;
                    while let Some(item) = overflow.pop_front() {
                        if tx.send(item).await.is_err() {
                            flushed = false;
                            break;
                        }
                    }
                    flushed
                }
                Overflow::Disk(overflow) => {
                    let mut flushed = true;
                    while overflow.len > 0 {
                        let item = overflow.pop().await.map_err(|e| {
                            TaskStreamError::from(CommonErrorCause::new::<PyErrExtractor>(&e))
                        });
                        let failed = item.is_err();
                        if tx.send(item).await.is_err() || failed {
                            flushed = false;
                            break;
                        }
                    }
                    flushed
                }
            };
            if flushed && let Some(error) = error {
                let _ = tx.send(Err(error)).await;
            }
        }
        self.tx = None;
        self.overflow.clear();
    }

    /// Ends the replica with an error after the batches that its reader has received.
    /// This never waits for the reader.
    fn fail(&mut self, error: TaskStreamError) {
        self.overflow.clear();
        let Some(tx) = self.tx.take() else {
            return;
        };
        if let Err(mpsc::error::TrySendError::Full(item)) = tx.try_send(Err(error))
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            // The error is sent once the reader reads the batches before it,
            // unless the reader is dropped.
            handle.spawn(async move {
                let _ = tx.send(item).await;
            });
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        // The task stops writing the replica, for example when it is canceled.
        self.fail(TaskStreamError::from(CommonErrorCause::Execution(
            "the task that writes the stream stopped before finishing it".to_string(),
        )));
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamReplicaSender {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let mut active = false;
        for replica in self.replicas.iter_mut() {
            let Some(tx) = replica.tx.as_ref() else {
                continue;
            };
            let dropped = match &mut replica.overflow {
                Overflow::Memory(overflow) => write_with_memory_overflow(tx, overflow, &batch),
                Overflow::Disk(overflow) => overflow.write(tx, &batch).await?,
            };
            // A dropped receiver can happen under normal operation when the receiver no longer
            // needs more data (e.g., after a LIMIT operator has received enough rows).
            if dropped {
                debug!("memory stream replica receiver has been dropped");
                replica.tx = None;
                replica.overflow.clear();
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
        // Flush them independently, finishing each replica as soon as it is flushed
        // so that its consumer sees EOF without waiting for the other replicas.
        join_all(
            self.replicas
                .into_iter()
                .map(|mut replica| async move { replica.finish(None).await }),
        )
        .await;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn fail(self: Box<Self>, error: &DataFusionError) -> Result<bool> {
        let error = TaskStreamError::from(CommonErrorCause::new::<PyErrExtractor>(error));
        if self.shared {
            // The error is the last item of every replica, so that it fails only the
            // readers that read the stream to its end, as they would fail if they
            // produced the stream themselves.
            join_all(self.replicas.into_iter().map(|mut replica| {
                let error = error.clone();
                async move { replica.finish(Some(error)).await }
            }))
            .await;
            Ok(true)
        } else {
            // Each reader fails with the error of the task after the batches it has
            // received, and the task fails as well.
            for mut replica in self.replicas {
                replica.fail(error.clone());
            }
            Ok(false)
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    fn context(memory_limit: Option<usize>, disk: bool) -> TaskContext {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = memory_limit {
            runtime = runtime.with_memory_limit(limit, 1.0);
        }
        if !disk {
            runtime = runtime.with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            );
        }
        TaskContext::default().with_runtime(runtime.build_arc().unwrap())
    }

    fn used_disk_space(context: &TaskContext) -> u64 {
        context.runtime_env().disk_manager.used_disk_space()
    }

    async fn write(sink: &mut Box<dyn TaskStreamChannelSink>, values: std::ops::Range<i64>) {
        for i in values {
            assert_eq!(
                sink.write(batch(i)).await.unwrap(),
                TaskStreamWriteState::Active
            );
        }
    }

    fn batch(i: i64) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![i]))]).unwrap()
    }

    fn value(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    /// Reads the replica to its end, and returns the values and the error at the end if any.
    async fn read(mut source: TaskStreamSource) -> (Vec<i64>, Option<String>) {
        let mut values = vec![];
        while let Some(item) = source.next().await {
            match item {
                Ok(batch) => values.push(value(&batch)),
                Err(error) => {
                    assert!(source.next().await.is_none());
                    return (values, Some(error.to_string()));
                }
            }
        }
        (values, None)
    }

    /// Creates a stream whose replicas have room for one batch, and writes three batches.
    async fn written(replicas: usize) -> (MemoryStream, Box<dyn TaskStreamChannelSink>) {
        let mut stream = MemoryStream::new(1, replicas, vec![]);
        let mut sink = stream.publish().unwrap();
        for i in 0..3 {
            assert_eq!(
                sink.write(batch(i)).await.unwrap(),
                TaskStreamWriteState::Active
            );
        }
        (stream, sink)
    }

    #[tokio::test]
    async fn a_failed_stream_ends_with_the_error_of_its_task() {
        let (mut stream, sink) = written(2).await;
        let error = DataFusionError::Execution("task failed".to_string());
        // The task does not wait for the readers, which have not read anything.
        let failed = tokio::time::timeout(Duration::from_secs(5), sink.fail(&error))
            .await
            .unwrap()
            .unwrap();
        assert!(!failed);
        for _ in 0..2 {
            let (values, error) = read(stream.subscribe().unwrap()).await;
            assert_eq!(values, vec![0]);
            assert!(error.unwrap().contains("task failed"));
        }
    }

    #[tokio::test]
    async fn a_stream_ends_without_error_only_when_it_is_committed() {
        let (mut stream, sink) = written(1).await;
        drop(sink);
        let (values, error) = read(stream.subscribe().unwrap()).await;
        assert_eq!(values, vec![0]);
        assert!(error.unwrap().contains("stopped before finishing"));

        let (mut stream, sink) = written(1).await;
        let reader = tokio::spawn(read(stream.subscribe().unwrap()));
        sink.commit().await.unwrap();
        assert_eq!(reader.await.unwrap(), (vec![0, 1, 2], None));
    }

    #[tokio::test]
    async fn a_replica_that_is_not_flushed_when_the_commit_is_canceled_ends_with_an_error() {
        let (mut stream, sink) = written(2).await;
        let reader = tokio::spawn(read(stream.subscribe().unwrap()));
        let unread = stream.subscribe().unwrap();
        // The commit waits for the replica that is not read.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), sink.commit())
                .await
                .is_err()
        );
        assert_eq!(reader.await.unwrap(), (vec![0, 1, 2], None));
        let (values, error) = read(unread).await;
        assert_eq!(values, vec![0]);
        assert!(error.is_some());
    }

    #[tokio::test]
    async fn a_failed_shared_stream_ends_with_the_error_after_all_its_batches() {
        let context = context(None, true);
        let mut stream = MemoryStream::new_shared(1, 2, vec![], &context, schema());
        let mut sink = stream.publish().unwrap();
        write(&mut sink, 0..3).await;
        let readers = (0..2)
            .map(|_| tokio::spawn(read(stream.subscribe().unwrap())))
            .collect::<Vec<_>>();
        let error = DataFusionError::Execution("task failed".to_string());
        assert!(sink.fail(&error).await.unwrap());
        for reader in readers {
            let (values, error) = reader.await.unwrap();
            assert_eq!(values, vec![0, 1, 2]);
            assert!(error.unwrap().contains("task failed"));
        }
    }

    #[tokio::test]
    async fn a_shared_stream_keeps_the_overflow_on_disk_only_under_a_memory_limit() {
        for (memory_limit, disk, spilled) in [
            (Some(1 << 20), true, true),
            (None, true, false),
            (Some(1 << 20), false, false),
        ] {
            let context = context(memory_limit, disk);
            let mut stream = MemoryStream::new_shared(1, 2, vec![], &context, schema());
            let mut sink = stream.publish().unwrap();
            write(&mut sink, 0..200).await;
            assert_eq!(used_disk_space(&context) > 0, spilled);
            assert_eq!(context.memory_pool().reserved(), 0);
            let readers = (0..2)
                .map(|_| tokio::spawn(read(stream.subscribe().unwrap())))
                .collect::<Vec<_>>();
            sink.commit().await.unwrap();
            for reader in readers {
                assert_eq!(reader.await.unwrap(), ((0..200).collect(), None));
            }
            assert_eq!(used_disk_space(&context), 0);
        }
    }

    #[tokio::test]
    async fn a_replica_reads_its_overflow_on_disk_while_it_is_written() {
        let context = context(Some(1 << 20), true);
        let mut stream = MemoryStream::new_shared(1, 1, vec![], &context, schema());
        let mut sink = stream.publish().unwrap();
        let mut source = stream.subscribe().unwrap();
        let mut values = vec![];
        // Each write moves one spilled batch to the replica once the replica has room.
        for round in 0..5 {
            write(&mut sink, round * 20..(round + 1) * 20).await;
            values.push(value(&source.next().await.unwrap().unwrap()));
        }
        assert!(used_disk_space(&context) > 0);
        let reader = tokio::spawn(read(source));
        sink.commit().await.unwrap();
        let (rest, error) = reader.await.unwrap();
        values.extend(rest);
        assert_eq!((values, error), ((0..100).collect(), None));
        assert_eq!(used_disk_space(&context), 0);
    }

    #[tokio::test]
    async fn a_dropped_replica_frees_its_overflow_on_disk() {
        let context = context(Some(1 << 20), true);
        let mut stream = MemoryStream::new_shared(1, 1, vec![], &context, schema());
        let mut sink = stream.publish().unwrap();
        write(&mut sink, 0..50).await;
        assert!(used_disk_space(&context) > 0);
        drop(stream.subscribe().unwrap());
        assert_eq!(
            sink.write(batch(50)).await.unwrap(),
            TaskStreamWriteState::Closed
        );
        assert_eq!(used_disk_space(&context), 0);
    }

    #[tokio::test]
    async fn a_task_reads_the_replicas_of_a_shared_stream_in_any_order() {
        for memory_limit in [None, Some(1 << 20)] {
            for first in 0..2 {
                let context = context(memory_limit, true);
                let mut stream = MemoryStream::new_shared(1, 2, vec![], &context, schema());
                let mut sink = stream.publish().unwrap();
                let mut sources = vec![stream.subscribe().unwrap(), stream.subscribe().unwrap()];
                let reader = tokio::spawn(async move {
                    // A self-join reads one side to its end before it reads the other side.
                    let a = read(sources.remove(first)).await;
                    let b = read(sources.remove(0)).await;
                    (a, b)
                });
                let writer = async move {
                    write(&mut sink, 0..1000).await;
                    sink.commit().await.unwrap();
                };
                let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
                    tokio::join!(reader, writer)
                })
                .await
                .unwrap();
                let (a, b) = result.unwrap();
                assert_eq!(a, ((0..1000).collect(), None));
                assert_eq!(b, ((0..1000).collect(), None));
            }
        }
    }
}
