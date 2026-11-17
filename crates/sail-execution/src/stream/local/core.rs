use std::collections::HashMap;
use std::collections::hash_map::Entry;

use datafusion::arrow::datatypes::SchemaRef;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::TaskStreamError;
use crate::stream::local::memory::{MemoryStream, MemoryStreamSender, memory_stream_channel};
use crate::stream::local::options::LocalStreamManagerOptions;
use crate::stream::local::{LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
            options,
            streams: HashMap::new(),
        }
    }

    pub fn create_stream(
        &mut self,
        key: TaskStreamKey,
        replicas: usize,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let create = |senders: Vec<_>| -> ExecutionResult<_> {
            let mut stream = Self::create_stream_with_senders(replicas, senders, &self.options);
            let sink = stream.publish()?;
            Ok((stream, sink))
        };

        match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let senders = match entry.get_mut() {
                    LocalStreamState::Created { .. } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream {} is already created",
                            TaskStreamKeyDisplay(&key)
                        )));
                    }
                    LocalStreamState::Pending { senders } => senders,
                    LocalStreamState::Failed { cause } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream creation has failed for {}: {}",
                            TaskStreamKeyDisplay(&key),
                            TaskStreamError::from(cause.clone())
                        )));
                    }
                };
                match create(senders.clone()) {
                    Ok((stream, sink)) => {
                        *entry.into_mut() = LocalStreamState::Created { stream };
                        Ok(sink)
                    }
                    Err(e) => {
                        let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                        Self::fail_senders(senders, &cause);
                        *entry.into_mut() = LocalStreamState::Failed { cause };
                        Err(e)
                    }
                }
            }
            Entry::Vacant(entry) => match create(vec![]) {
                Ok((stream, sink)) => {
                    entry.insert(LocalStreamState::Created { stream });
                    Ok(sink)
                }
                Err(e) => {
                    let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                    entry.insert(LocalStreamState::Failed { cause });
                    Err(e)
                }
            },
        }
    }

    pub fn fetch_stream(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource> {
        match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                LocalStreamState::Created { stream } => stream.subscribe(key),
                LocalStreamState::Pending { senders } => {
                    let (tx, rx) = memory_stream_channel(self.options.task_stream_buffer, key);
                    senders.push(tx);
                    // There is no need to probe the pending stream again.
                    Ok(rx)
                }
                LocalStreamState::Failed { cause } => Err(ExecutionError::InternalError(format!(
                    "local stream creation has failed for {}: {}",
                    TaskStreamKeyDisplay(key),
                    TaskStreamError::from(cause.clone())
                ))),
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = memory_stream_channel(self.options.task_stream_buffer, key);
                entry.insert(LocalStreamState::Pending { senders: vec![tx] });
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                Ok(rx)
            }
        }
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        if let Some(stage) = stage {
            self.streams
                .retain(|key, _| key.job_id != job_id || key.stage != stage);
        } else {
            self.streams.retain(|key, _| key.job_id != job_id);
        }
    }

    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey) {
        let Some(value) = self.streams.get_mut(key) else {
            return;
        };
        if let LocalStreamState::Pending { senders } = value {
            let message = "local stream is not created within the expected time".to_string();
            let cause = CommonErrorCause::Execution(message);
            Self::fail_senders(senders, &cause);
            *value = LocalStreamState::Failed { cause };
        }
    }

    pub fn fail_senders(senders: &[MemoryStreamSender], cause: &CommonErrorCause) {
        for tx in senders {
            // `try_send` would not fail due to full buffer because we have
            // never sent any data to the channel.
            // So we do not need to spawn a task to send the error asynchronously.
            let _ = tx.try_send(Err(TaskStreamError::from(cause.clone())));
        }
    }

    fn create_stream_with_senders(
        replicas: usize,
        senders: Vec<MemoryStreamSender>,
        options: &LocalStreamManagerOptions,
    ) -> MemoryStream {
        MemoryStream::new(options.task_stream_buffer, replicas, senders)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Poll;

    use datafusion::arrow::datatypes::Schema;
    use futures::{StreamExt, poll};
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeHandle;

    use super::*;
    use crate::driver::DriverOptions;
    use crate::id::DriverId;

    fn manager() -> ExecutionResult<LocalStreamManager> {
        let config = AppConfig::load().map_err(|e| ExecutionError::InternalError(e.to_string()))?;
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let options =
            DriverOptions::try_new(&config, runtime, "test".into(), DriverId::from(1), 0)?;
        Ok(LocalStreamManager::new((&options).into()))
    }

    fn key(stage: usize) -> TaskStreamKey {
        TaskStreamKey {
            job_id: JobId::from(1),
            stage,
            partition: 0,
            attempt: 0,
            channel: 0,
        }
    }

    #[tokio::test]
    async fn removed_or_dropped_streams_fail_attached_readers() -> ExecutionResult<()> {
        for drop_manager in [false, true] {
            let mut manager = manager()?;
            // A reader of a stream whose producer went away without committing.
            let sink = manager.create_stream(key(0), 1, Arc::new(Schema::empty()))?;
            let Some(LocalStreamState::Created { stream }) = manager.streams.get_mut(&key(0))
            else {
                return Err(ExecutionError::InternalError("missing stream".into()));
            };
            let mut uncommitted = stream.subscribe(&key(0))?;
            drop(sink);
            // A reader of a stream that has not been created.
            let (tx, mut pending) = memory_stream_channel(1, &key(1));
            manager
                .streams
                .insert(key(1), LocalStreamState::Pending { senders: vec![tx] });
            assert!(poll!(uncommitted.next()).is_pending());
            assert!(poll!(pending.next()).is_pending());

            if drop_manager {
                drop(manager);
            } else {
                manager.remove_streams(JobId::from(1), None);
            }
            for (stage, reader) in [(0, &mut uncommitted), (1, &mut pending)] {
                let message = format!(
                    "task stream for {} was closed before its producer finished",
                    TaskStreamKeyDisplay(&key(stage))
                );
                assert!(matches!(
                    poll!(reader.next()),
                    Poll::Ready(Some(Err(error))) if error.to_string() == message
                ));
                assert!(matches!(poll!(reader.next()), Poll::Ready(None)));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_pending_stream_ends_with_cause() {
        let (tx, mut reader) = memory_stream_channel(1, &key(0));
        let cause = CommonErrorCause::Execution("test failure".to_string());
        LocalStreamManager::fail_senders(&[tx], &cause);
        assert!(matches!(
            poll!(reader.next()),
            Poll::Ready(Some(Err(error))) if error.to_string().contains("test failure")
        ));
        assert!(matches!(poll!(reader.next()), Poll::Ready(None)));
    }
}
