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
                match create(std::mem::take(senders)) {
                    Ok((stream, sink)) => {
                        *entry.into_mut() = LocalStreamState::Created { stream };
                        Ok(sink)
                    }
                    Err(e) => {
                        let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
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
                LocalStreamState::Created { stream } => stream.subscribe(),
                LocalStreamState::Pending { senders } => {
                    let (tx, rx) = memory_stream_channel(self.options.task_stream_buffer);
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
                let (tx, rx) = memory_stream_channel(self.options.task_stream_buffer);
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
            Self::fail_senders(std::mem::take(senders), &cause);
            *value = LocalStreamState::Failed { cause };
        }
    }

    pub fn fail_senders(senders: Vec<MemoryStreamSender>, cause: &CommonErrorCause) {
        for tx in senders {
            tx.finish(Err(TaskStreamError::from(cause.clone())));
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

    use datafusion::arrow::datatypes::Schema;
    use futures::TryStreamExt;
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeHandle;

    use super::*;
    use crate::driver::DriverOptions;

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn pending_cleanup_timeout_and_retry_are_not_successful_eof()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut config = AppConfig::load()?;
        config.cluster.task_stream_buffer = 1;
        let handle = tokio::runtime::Handle::current();
        let options = DriverOptions::try_new(
            &config,
            RuntimeHandle::new(handle.clone(), handle),
            "stream-completion".into(),
            1.into(),
            0,
        )?;
        let mut manager = LocalStreamManager::new(LocalStreamManagerOptions::from(&options));
        let key = TaskStreamKey {
            job_id: 1.into(),
            stage: 0,
            partition: 0,
            attempt: 0,
            channel: 0,
        };
        for timeout in [false, true] {
            let (sender, mut source) = memory_stream_channel(1);
            manager.streams.insert(
                key.clone(),
                LocalStreamState::Pending {
                    senders: vec![sender],
                },
            );
            if timeout {
                manager.fail_stream_if_pending(&key);
            } else {
                manager.remove_streams(key.job_id, None);
            }
            assert!(source.try_next().await.is_err());
            manager.remove_streams(key.job_id, None);
        }
        let (sender, mut failed) = memory_stream_channel(1);
        manager.streams.insert(
            key.clone(),
            LocalStreamState::Pending {
                senders: vec![sender],
            },
        );
        let first = manager.create_stream(key.clone(), 1, Arc::new(Schema::empty()))?;
        let retry_key = TaskStreamKey { attempt: 1, ..key };
        let (sender, mut retry) = memory_stream_channel(1);
        manager.streams.insert(
            retry_key.clone(),
            LocalStreamState::Pending {
                senders: vec![sender],
            },
        );
        let second = manager.create_stream(retry_key, 1, Arc::new(Schema::empty()))?;
        first
            .abort(CommonErrorCause::Io("missing file".into()).into())
            .await?;
        second.commit().await?;
        assert!(
            matches!(CommonErrorCause::from(failed.try_next().await.expect_err("failed attempt")), CommonErrorCause::Io(s) if s == "missing file")
        );
        assert!(retry.try_next().await?.is_none());
        Ok(())
    }
}
