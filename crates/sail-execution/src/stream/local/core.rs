use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::SchemaRef;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::TaskStreamError;
use crate::stream::local::memory::MemoryStream;
use crate::stream::local::options::LocalStreamManagerOptions;
use crate::stream::local::{LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
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
        job_output: bool,
        _schema: SchemaRef,
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
                Some(LocalStreamState::Pending { waiters }) => waiters,
                _ => vec![],
            })
            .collect();
        // The result of a job is read at the pace of the client, and the tasks that
        // produce it must not keep their resources while the client is slow or away.
        // The streams of a stage are removed once, when its readers have finished.
        // The streams that are created afterwards may never be read or removed.
        // The sink does not wait for readers in these cases.
        let bound = if job_output || self.cleaned.contains(&(key.job_id, key.stage)) {
            usize::MAX
        } else {
            self.options.task_stream_buffer
        };
        let (sink, streams) = MemoryStream::create(waiters, replicas, bound);
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
                LocalStreamState::Pending { waiters } => {
                    // There is no need to probe the pending stream again, unless the
                    // earlier probe has left the stream alone since all readers had left.
                    if waiters.iter().all(|waiter| waiter.is_closed()) {
                        ctx.send_with_delay(
                            TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
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
                });
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                Ok(source)
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

    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey) {
        let Some(value) = self.streams.get_mut(key) else {
            return;
        };
        if let LocalStreamState::Pending { waiters } = value {
            // Readers that have left do not need the stream. Their replicas are closed
            // when the stream is created, which can happen at any later time.
            if waiters.iter().all(|waiter| waiter.is_closed()) {
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

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::prelude::SessionContext;
    use futures::{FutureExt, StreamExt};
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeHandle;

    use super::*;
    use crate::stream::writer::TaskStreamWriteState;
    use crate::worker::WorkerOptions;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Creates a manager whose sinks wait when a replica holds more than one batch.
    fn manager() -> Result<LocalStreamManager, Box<dyn std::error::Error>> {
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let mut config = AppConfig::load()?;
        config.cluster.task_stream_buffer = 1;
        let options = WorkerOptions::new(&config, runtime, SessionContext::new());
        Ok(LocalStreamManager::new((&options).into()))
    }

    fn task_key(partition: usize) -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 0,
            partition,
            attempt: 0,
        }
    }

    fn batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    /// Writes more batches than a sink that waits for readers would accept.
    fn write_without_waiting(sink: &mut Box<dyn TaskStreamSink>) -> bool {
        (0..10).all(|_| {
            let state = sink.write(vec![Some(batch())]).now_or_never();
            matches!(state, Some(Ok(TaskStreamWriteState::Active)))
        })
    }

    /// Adds a reader for a stream that has not been created, as `fetch_stream` does.
    fn fetch_pending(manager: &mut LocalStreamManager, key: TaskStreamKey) -> TaskStreamSource {
        let (waiter, source) = MemoryStream::pending();
        manager.streams.insert(
            key,
            LocalStreamState::Pending {
                waiters: vec![waiter],
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
        let mut sink = manager.create_stream(&key, 1, 1, false, schema.clone())?;
        assert!(write_without_waiting(&mut sink));
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
        let mut sink = manager.create_stream(&other, 1, 1, false, schema.clone())?;
        assert!(!write_without_waiting(&mut sink));

        // The cleanup of the job ends the special case for its stages.
        manager.remove_streams(key.job_id, None);
        assert!(manager.cleaned.is_empty());
        let mut sink = manager.create_stream(&key, 1, 1, false, schema)?;
        assert!(!write_without_waiting(&mut sink));
        Ok(())
    }

    #[tokio::test]
    async fn streams_of_job_output_never_wait() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());
        let mut sink = manager.create_stream(&task_key(0), 1, 1, true, schema.clone())?;
        assert!(write_without_waiting(&mut sink));
        let mut sink = manager.create_stream(&task_key(1), 1, 1, false, schema)?;
        assert!(!write_without_waiting(&mut sink));
        Ok(())
    }

    #[tokio::test]
    async fn stage_cleanup_closes_streams_for_readers_that_have_left() -> TestResult {
        let mut manager = manager()?;
        let schema = Arc::new(Schema::empty());

        // The stream exists when the cleanup happens.
        let key = task_key(0);
        let mut sink = manager.create_stream(&key, 1, 1, false, schema.clone())?;
        manager.remove_streams(key.job_id, Some(key.stage));
        let state = sink.write(vec![Some(batch())]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // The stream is created after the cleanup. Its reader has requested it and left.
        let key = task_key(1);
        drop(fetch_pending(&mut manager, key.task_stream_key(0)));
        manager.remove_streams(key.job_id, Some(key.stage));
        let mut sink = manager.create_stream(&key, 1, 1, false, schema.clone())?;
        let state = sink.write(vec![Some(batch())]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A reader that is still waiting for the stream gets its data.
        let key = task_key(2);
        let mut reader = fetch_pending(&mut manager, key.task_stream_key(0));
        manager.remove_streams(key.job_id, Some(key.stage));
        let mut sink = manager.create_stream(&key, 1, 1, false, schema)?;
        let state = sink.write(vec![Some(batch())]).await?;
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
        let batch = RecordBatch::new_empty(schema.clone());

        // The reader leaves before the stream is created. The stream can still be
        // created later, and the replica for that reader is closed.
        let key = task_key(0);
        drop(fetch_pending(&mut manager, key.task_stream_key(0)));
        manager.fail_stream_if_pending(&key.task_stream_key(0));
        let mut sink = manager.create_stream(&key, 1, 1, false, schema.clone())?;
        let state = sink.write(vec![Some(batch)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A reader that is still waiting gets the error, and the stream cannot be created.
        let key = task_key(1);
        let mut reader = fetch_pending(&mut manager, key.task_stream_key(0));
        manager.fail_stream_if_pending(&key.task_stream_key(0));
        assert!(matches!(reader.next().await, Some(Err(_))));
        assert!(reader.next().await.is_none());
        assert!(manager.create_stream(&key, 1, 1, false, schema).is_err());
        Ok(())
    }
}
