use std::collections::HashMap;
use std::collections::hash_map::Entry;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::local::memory::MemoryStream;
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
                LocalStreamState::Created { stream } => stream.subscribe(),
                LocalStreamState::Pending { senders } => {
                    let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                    senders.push(tx);
                    // There is no need to probe the pending stream again.
                    Ok(Box::pin(ReceiverStream::new(rx)))
                }
                LocalStreamState::Failed { cause } => Err(ExecutionError::InternalError(format!(
                    "local stream creation has failed for {}: {}",
                    TaskStreamKeyDisplay(key),
                    TaskStreamError::from(cause.clone())
                ))),
            },
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                entry.insert(LocalStreamState::Pending { senders: vec![tx] });
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                Ok(Box::pin(ReceiverStream::new(rx)))
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

    pub fn fail_senders(
        senders: &[mpsc::Sender<TaskStreamResult<RecordBatch>>],
        cause: &CommonErrorCause,
    ) {
        for tx in senders {
            // `try_send` would not fail due to full buffer because we have
            // never sent any data to the channel.
            // So we do not need to spawn a task to send the error asynchronously.
            let _ = tx.try_send(Err(TaskStreamError::from(cause.clone())));
        }
    }

    fn create_stream_with_senders(
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        options: &LocalStreamManagerOptions,
    ) -> MemoryStream {
        MemoryStream::new(options.task_stream_buffer, replicas, senders)
    }
}
