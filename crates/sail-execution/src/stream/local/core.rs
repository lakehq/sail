use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use log::info;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, TaskStreamKeyDisplay, TaskStreamKeySet};
use crate::stream::error::TaskStreamError;
use crate::stream::local::memory::MemoryStream;
use crate::stream::local::options::LocalStreamManagerOptions;
use crate::stream::local::process_memory::available_memory;
use crate::stream::local::queue::{QueueLimit, StreamMemory};
use crate::stream::local::{LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

/// The share of the memory available to the process that its task streams can hold
/// by default before tasks wait for the readers of their output, as a numerator and
/// a denominator. The rest is for everything that is not counted: the operators of
/// the tasks, data in transit, the memory that the allocator holds beyond the data
/// of the streams, and other processes in the same container.
const DEFAULT_CEILING_FRACTION: (u64, u64) = (1, 3);

/// Returns the default number of bytes that the task streams of a process can hold
/// before tasks wait for the readers of their output, given the memory available to
/// the process. The task streams are not limited if the memory is not known.
fn default_ceiling(available: Option<u64>) -> usize {
    let (numerator, denominator) = DEFAULT_CEILING_FRACTION;
    available.map_or(usize::MAX, |available| {
        usize::try_from((available / denominator).saturating_mul(numerator)).unwrap_or(usize::MAX)
    })
}

/// Returns the number of bytes that the task streams of this process can hold
/// before tasks wait for the readers of their output, given the configured limit.
/// The memory available to the process is read once, when a default is first needed.
fn process_ceiling(limit: usize) -> usize {
    static DEFAULT: OnceLock<usize> = OnceLock::new();
    match limit {
        0 => *DEFAULT.get_or_init(|| {
            let available = available_memory();
            let ceiling = default_ceiling(available);
            let (numerator, denominator) = DEFAULT_CEILING_FRACTION;
            match available {
                Some(available) => info!(
                    "task streams can hold {ceiling} bytes before tasks wait for their readers: \
                    {numerator}/{denominator} of the {available} bytes of memory of the process"
                ),
                None => info!("task streams are not limited: the memory of the process is unknown"),
            }
            ceiling
        }),
        limit => limit,
    }
}

/// Returns the memory of the task streams of this process. Memory belongs to
/// a process, so the driver and the workers that run in the same process count
/// their streams together.
fn process_stream_memory() -> Arc<StreamMemory> {
    static MEMORY: OnceLock<Arc<StreamMemory>> = OnceLock::new();
    MEMORY.get_or_init(Arc::default).clone()
}

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
            memory: process_stream_memory(),
            ceiling: process_ceiling(options.task_stream_memory_limit),
            options,
            streams: HashMap::new(),
            cleaned: HashSet::new(),
        }
    }

    /// Creates the streams for all channels of a task.
    /// The returned sink writes to all of them.
    pub fn create_stream(
        &mut self,
        key: &TaskKey,
        channels: usize,
        replicas: usize,
        bounded: bool,
        schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let keys = (0..channels)
            .map(|channel| key.task_stream_key(channel))
            .collect::<Vec<_>>();
        for key in &keys {
            match self.streams.get(key) {
                Some(LocalStreamState::Created { .. }) => {
                    return Err(ExecutionError::InternalError(format!(
                        "local stream {} is already created",
                        TaskStreamKeyDisplay(key)
                    )));
                }
                Some(LocalStreamState::Failed { cause }) => {
                    return Err(ExecutionError::InternalError(format!(
                        "local stream creation has failed for {}: {}",
                        TaskStreamKeyDisplay(key),
                        TaskStreamError::from(cause.clone())
                    )));
                }
                Some(LocalStreamState::Pending { .. }) | None => {}
            }
        }
        let waiters = keys
            .iter()
            .map(|key| match self.streams.remove(key) {
                Some(LocalStreamState::Pending { waiters, .. }) => waiters,
                _ => vec![],
            })
            .collect();
        // The sink can wait for the readers only if the driver has found that each stream
        // has a single reader that takes the data as it comes. This is not the case for
        // the result of a job, which is read at the pace of the client, when the reader
        // decides which of its inputs it reads next, or when the streams have several
        // replicas, some of which may never be read. Besides, the sink never waits
        // when the streams are created after the streams of the stage have been
        // removed, since nobody may ever read or remove them.
        let bounded = bounded && !self.cleaned.contains(&(key.job_id, key.stage));
        let buffer = self.options.task_stream_buffer;
        let limit = bounded.then(|| QueueLimit {
            rows: buffer.saturating_mul(self.options.batch_size),
            ceiling: self.ceiling,
        });
        let memory = self.memory.clone();
        let (sink, streams) = MemoryStream::create(key, waiters, replicas, limit, memory, schema);
        for (key, stream) in keys.into_iter().zip(streams) {
            self.streams
                .insert(key, LocalStreamState::Created { stream });
        }
        Ok(sink)
    }

    pub fn fetch_stream(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource> {
        match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                LocalStreamState::Created { stream } => stream.subscribe(),
                LocalStreamState::Pending { waiters, probes } => {
                    // There is no need to probe the pending stream again, unless all
                    // earlier readers have left. The earlier probes are then ignored.
                    if waiters.iter().all(|waiter| waiter.is_closed()) {
                        *probes += 1;
                        ctx.send_with_delay(
                            TaskRunnerMessage::ProbePendingLocalStream {
                                key: key.clone(),
                                probe: *probes,
                            },
                            self.options.task_stream_creation_timeout,
                        );
                    }
                    let (waiter, source) = MemoryStream::pending();
                    waiters.push(waiter);
                    Ok(source)
                }
                LocalStreamState::Failed { cause } => Err(ExecutionError::InternalError(format!(
                    "local stream creation has failed for {}: {}",
                    TaskStreamKeyDisplay(key),
                    TaskStreamError::from(cause.clone())
                ))),
            },
            Entry::Vacant(entry) => {
                let (waiter, source) = MemoryStream::pending();
                entry.insert(LocalStreamState::Pending {
                    waiters: vec![waiter],
                    probes: 1,
                });
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream {
                        key: key.clone(),
                        probe: 1,
                    },
                    self.options.task_stream_creation_timeout,
                );
                Ok(source)
            }
        }
    }

    /// Fetches several streams for one reader, which reads them in arbitrary order.
    /// Each stream is fetched on its own, so a stream that is not created yet does not
    /// delay the others. If one of the streams cannot be fetched, the reader gets the
    /// error of the first such stream. All the other streams are still fetched, so
    /// that the reader gives up its place in each of them, and the tasks that write
    /// them learn that it does not read them. A reader that releases the streams
    /// gives up its place in each of them and gets no data.
    pub fn fetch_streams(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        keys: &TaskStreamKeySet,
    ) -> ExecutionResult<TaskStreamSource> {
        if keys.release {
            for key in keys.keys() {
                self.release_stream(key);
            }
            return Ok(Box::pin(futures::stream::empty()));
        }
        let mut sources = vec![];
        let mut error = None;
        for key in keys.keys() {
            match self.fetch_stream(ctx, &key) {
                Ok(source) => sources.push(source),
                Err(e) => error = error.or(Some(e)),
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        if sources.len() == 1 {
            return Ok(sources.swap_remove(0));
        }
        Ok(Box::pin(futures::stream::select_all(sources)))
    }

    /// Gives up the place of a reader in a stream. The task that writes the stream
    /// learns so at once, or when it creates the stream. A stream that is not created
    /// yet is not probed, since the reader does not wait for it.
    fn release_stream(&mut self, key: TaskStreamKey) {
        // The reader of this source has left.
        let (waiter, _) = MemoryStream::pending();
        match self.streams.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                LocalStreamState::Created { stream } => {
                    let _ = stream.subscribe();
                }
                LocalStreamState::Pending { waiters, .. } => waiters.push(waiter),
                LocalStreamState::Failed { .. } => {}
            },
            Entry::Vacant(entry) => {
                entry.insert(LocalStreamState::Pending {
                    waiters: vec![waiter],
                    probes: 0,
                });
            }
        }
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        if let Some(stage) = stage {
            // The readers that have requested a stream before its creation are kept.
            // If they have left, their replicas are closed when the stream is created.
            self.streams.retain(|key, value| {
                key.job_id != job_id
                    || key.stage != stage
                    || matches!(value, LocalStreamState::Pending { .. })
            });
            self.cleaned.insert((job_id, stage));
        } else {
            self.streams.retain(|key, _| key.job_id != job_id);
            self.cleaned.retain(|(id, _)| *id != job_id);
        }
    }

    /// Fails a stream that is not created yet, for the given probe of the stream.
    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey, probe: usize) {
        let Some(value) = self.streams.get_mut(key) else {
            return;
        };
        if let LocalStreamState::Pending { waiters, probes } = value {
            // Readers that have left do not need the stream. Their replicas are closed
            // when the stream is created, which can happen at any later time.
            // A probe that was scheduled for them does not count for later readers.
            if *probes != probe || waiters.iter().all(|waiter| waiter.is_closed()) {
                return;
            }
            let message = "local stream is not created within the expected time".to_string();
            let cause = CommonErrorCause::Execution(message);
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Err(TaskStreamError::from(cause.clone())));
            }
            *value = LocalStreamState::Failed { cause };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::error::ArrowError;
    use datafusion::prelude::SessionContext;
    use futures::{FutureExt, StreamExt};
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeHandle;

    use super::*;
    use crate::stream::writer::TaskStreamWriteState;
    use crate::worker::WorkerOptions;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Creates a manager whose sinks can always queue one batch of one row, and
    /// wait beyond that when the task streams hold the given number of bytes.
    fn manager_with_memory_limit(
        limit: usize,
    ) -> Result<LocalStreamManager, Box<dyn std::error::Error>> {
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let mut config = AppConfig::load()?;
        config.execution.batch_size = 1;
        config.cluster.task_stream_buffer = 1;
        config.cluster.task_stream_memory_limit = limit;
        let options = WorkerOptions::new(&config, runtime, SessionContext::new());
        let mut manager = LocalStreamManager::new((&options).into());
        // The tests do not count their streams together.
        manager.memory = Arc::default();
        Ok(manager)
    }

    /// Creates a manager whose sinks wait when their streams have one row.
    fn manager() -> Result<LocalStreamManager, Box<dyn std::error::Error>> {
        manager_with_memory_limit(1)
    }

    fn task_key(partition: usize) -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 0,
            partition,
            attempt: 0,
        }
    }

    /// Creates a batch with one row and no columns.
    fn batch() -> Result<RecordBatch, ArrowError> {
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
    }

    /// Writes more batches than a sink that waits for readers would accept.
    fn write_without_waiting(sink: &mut Box<dyn TaskStreamSink>) -> Result<bool, ArrowError> {
        for _ in 0..10 {
            let state = sink.write(vec![Some(batch()?)]).now_or_never();
            if !matches!(state, Some(Ok(TaskStreamWriteState::Active))) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Adds a reader for a stream that has not been created, as `fetch_stream` does.
    fn fetch_pending(manager: &mut LocalStreamManager, key: TaskStreamKey) -> TaskStreamSource {
        let (waiter, source) = MemoryStream::pending();
        manager.streams.insert(
            key,
            LocalStreamState::Pending {
                waiters: vec![waiter],
                probes: 1,
            },
        );
        source
    }

    #[tokio::test]
    async fn streams_created_after_stage_cleanup_never_wait() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());

        // Nobody may ever read or remove the streams of this task, since the cleanup
        // of its stage has already happened. This also applies to later attempts.
        let key = TaskKey {
            attempt: 1,
            ..task_key(0)
        };
        manager.remove_streams(key.job_id, Some(key.stage));
        let mut sink = manager.create_stream(&key, 1, 1, true, schema.clone())?;
        assert!(write_without_waiting(&mut sink)?);
        assert!(matches!(sink.commit().now_or_never(), Some(Ok(()))));

        // The streams are not closed, so that a reader that does come gets all the data.
        let Some(LocalStreamState::Created { stream }) =
            manager.streams.get_mut(&key.task_stream_key(0))
        else {
            return Err("the stream is not created".into());
        };
        let mut reader = stream.subscribe()?;
        for _ in 0..10 {
            assert!(matches!(reader.next().await, Some(Ok(_))));
        }
        assert!(reader.next().await.is_none());

        // The other stages of the job are not affected.
        let other = TaskKey {
            stage: key.stage + 1,
            ..key.clone()
        };
        let mut sink = manager.create_stream(&other, 1, 1, true, schema.clone())?;
        assert!(!write_without_waiting(&mut sink)?);

        // The cleanup of the job ends the special case for its stages.
        manager.remove_streams(key.job_id, None);
        assert!(manager.cleaned.is_empty());
        let mut sink = manager.create_stream(&key, 1, 1, true, schema)?;
        assert!(!write_without_waiting(&mut sink)?);
        Ok(())
    }

    #[tokio::test]
    async fn streams_wait_only_when_bounded() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());
        let mut sink = manager.create_stream(&task_key(0), 1, 1, false, schema.clone())?;
        assert!(write_without_waiting(&mut sink)?);
        let mut sink = manager.create_stream(&task_key(1), 1, 1, true, schema)?;
        assert!(!write_without_waiting(&mut sink)?);
        Ok(())
    }

    #[tokio::test]
    async fn streams_wait_only_at_the_memory_limit() -> TestResult {
        let schema = Arc::new(Schema::empty());
        let mut manager = manager_with_memory_limit(0)?;
        let mut sink = manager.create_stream(&task_key(0), 1, 1, true, schema.clone())?;
        assert!(write_without_waiting(&mut sink)?);
        // Each batch counts as one byte.
        let mut manager = manager_with_memory_limit(10)?;
        let mut sink = manager.create_stream(&task_key(0), 1, 1, true, schema.clone())?;
        assert!(write_without_waiting(&mut sink)?);
        let mut manager = manager_with_memory_limit(9)?;
        let mut sink = manager.create_stream(&task_key(0), 1, 1, true, schema)?;
        assert!(!write_without_waiting(&mut sink)?);
        Ok(())
    }

    #[test]
    fn default_memory_limit_is_a_share_of_the_memory_of_the_process() -> TestResult {
        let (numerator, denominator) = DEFAULT_CEILING_FRACTION;
        let available = 64 << 30;
        let ceiling = u64::try_from(default_ceiling(Some(available)))?;
        assert!(0 < ceiling && ceiling <= available);
        assert_eq!(ceiling, available / denominator * numerator);
        assert_eq!(default_ceiling(None), usize::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_is_the_configured_number_or_the_default() -> TestResult {
        assert_eq!(manager_with_memory_limit(1)?.ceiling, 1);
        assert_eq!(manager_with_memory_limit(1 << 20)?.ceiling, 1 << 20);
        // The memory of the process is known on these systems.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(manager_with_memory_limit(0)?.ceiling < usize::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn stage_cleanup_closes_streams_for_readers_that_have_left() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());

        // The stream exists when the cleanup happens.
        let key = task_key(0);
        let mut sink = manager.create_stream(&key, 1, 1, true, schema.clone())?;
        manager.remove_streams(key.job_id, Some(key.stage));
        let state = sink.write(vec![Some(batch()?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // The stream is created after the cleanup. Its reader has requested it and left.
        let key = task_key(1);
        drop(fetch_pending(&mut manager, key.task_stream_key(0)));
        manager.remove_streams(key.job_id, Some(key.stage));
        let mut sink = manager.create_stream(&key, 1, 1, true, schema.clone())?;
        let state = sink.write(vec![Some(batch()?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A reader that is still waiting for the stream gets its data.
        let key = task_key(2);
        let mut reader = fetch_pending(&mut manager, key.task_stream_key(0));
        manager.remove_streams(key.job_id, Some(key.stage));
        let mut sink = manager.create_stream(&key, 1, 1, true, schema)?;
        let state = sink.write(vec![Some(batch()?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        sink.commit().await?;
        assert!(matches!(reader.next().await, Some(Ok(_))));
        assert!(reader.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn creation_timeout_ignores_readers_that_have_left() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());

        // The reader leaves before the stream is created. The stream can still be
        // created later, and the replica for that reader is closed.
        let key = task_key(0);
        drop(fetch_pending(&mut manager, key.task_stream_key(0)));
        manager.fail_stream_if_pending(&key.task_stream_key(0), 1);
        let mut sink = manager.create_stream(&key, 1, 1, true, schema.clone())?;
        let state = sink.write(vec![Some(batch()?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A reader that is still waiting gets the error, and the stream cannot be created.
        let key = task_key(1);
        let mut reader = fetch_pending(&mut manager, key.task_stream_key(0));
        manager.fail_stream_if_pending(&key.task_stream_key(0), 1);
        assert!(matches!(reader.next().await, Some(Err(_))));
        assert!(reader.next().await.is_none());
        assert!(manager.create_stream(&key, 1, 1, true, schema).is_err());
        Ok(())
    }
}
