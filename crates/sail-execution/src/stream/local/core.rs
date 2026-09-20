use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use futures::TryStreamExt;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::TaskStreamError;
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
        schema: SchemaRef,
        context: &TaskContext,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let senders = match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                LocalStreamState::Pending { senders } => std::mem::take(senders),
                LocalStreamState::Created { .. } => {
                    return Err(ExecutionError::InternalError(format!(
                        "local stream {} is already created",
                        TaskStreamKeyDisplay(&key),
                    )));
                }
                LocalStreamState::Failed { cause } => {
                    return Err(ExecutionError::InternalError(format!(
                        "local stream creation has failed for {}: {}",
                        TaskStreamKeyDisplay(&key),
                        TaskStreamError::from(cause.clone()),
                    )));
                }
            },
            Entry::Vacant(_) => vec![],
        };
        let mut stream = MemoryStream::new(
            context,
            schema,
            key.partition,
            self.options.task_stream_buffer,
            replicas.max(senders.len()),
        )?;
        let sink = stream.publish()?;
        for sender in senders {
            // Sending to an already-cancelled waiter drops its reserved cursor.
            let _ = sender.send(
                stream
                    .subscribe()
                    .map_err(|error| TaskStreamError::External(Arc::new(error))),
            );
        }
        self.streams
            .insert(key, LocalStreamState::Created { stream });
        Ok(sink)
    }

    pub fn fetch_stream(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource> {
        let senders = match self.streams.entry(key.clone()) {
            Entry::Occupied(entry) => match entry.into_mut() {
                LocalStreamState::Created { stream } => return stream.subscribe(),
                LocalStreamState::Pending { senders } => senders,
                LocalStreamState::Failed { cause } => {
                    return Err(ExecutionError::InternalError(format!(
                        "local stream creation has failed for {}: {}",
                        TaskStreamKeyDisplay(key),
                        TaskStreamError::from(cause.clone()),
                    )));
                }
            },
            Entry::Vacant(entry) => {
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                let LocalStreamState::Pending { senders } =
                    entry.insert(LocalStreamState::Pending { senders: vec![] })
                else {
                    return Err(ExecutionError::InternalError(
                        "expected pending stream".into(),
                    ));
                };
                senders
            }
        };
        let (tx, rx) = oneshot::channel();
        senders.push(tx);
        Ok(Box::pin(
            futures::stream::once(async move {
                rx.await
                    .map_err(|error| TaskStreamError::External(Arc::new(error)))?
            })
            .try_flatten(),
        ))
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
            let cause = CommonErrorCause::Execution(
                "local stream is not created within the expected time".to_string(),
            );
            for sender in std::mem::take(senders) {
                let _ = sender.send(Err(TaskStreamError::from(cause.clone())));
            }
            *value = LocalStreamState::Failed { cause };
        }
    }
}
