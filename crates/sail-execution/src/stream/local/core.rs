use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use futures::TryStreamExt;
use sail_common::actor::ActorContext;
use tokio::sync::oneshot;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::TaskStreamError;
use crate::stream::local::channel::ChannelStreamWriter;
use crate::stream::local::memory::MemoryStream;
use crate::stream::local::options::LocalStreamManagerOptions;
use crate::stream::local::{LocalStream, LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

impl LocalStream {
    fn subscribe(&mut self) -> ExecutionResult<TaskStreamSource> {
        match self {
            Self::Replayable(stream) => Ok(stream.subscribe()),
            Self::Single(source) => source.take().ok_or_else(|| {
                ExecutionError::InternalError("local stream already has a consumer".into())
            }),
        }
    }
}

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
            options,
            streams: HashMap::new(),
        }
    }

    pub fn buffer(&self) -> usize {
        self.options.task_stream_buffer
    }

    pub fn create_stream(
        &mut self,
        key: TaskStreamKey,
        replayable: bool,
        context: &TaskContext,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let state = self
            .streams
            .entry(key.clone())
            .or_insert_with(|| LocalStreamState::Pending {
                subscribers: vec![],
            });
        let LocalStreamState::Pending { subscribers } = state else {
            return Err(ExecutionError::InternalError(format!(
                "local stream {} is already created or failed",
                TaskStreamKeyDisplay(&key)
            )));
        };
        let (mut stream, sink): (_, Box<dyn TaskStreamChannelSink>) = if replayable {
            let mut stream = MemoryStream::new(self.options.task_stream_buffer);
            let sink = stream.publish(context.runtime_env().disk_manager.clone())?;
            (LocalStream::Replayable(stream), Box::new(sink))
        } else {
            let (sink, source) = ChannelStreamWriter::new();
            (LocalStream::Single(Some(source)), Box::new(sink))
        };
        for subscriber in subscribers.drain(..) {
            if !subscriber.is_closed() {
                let _ = subscriber.send(stream.subscribe());
            }
        }
        *state = LocalStreamState::Created(stream);
        Ok(sink)
    }

    pub fn fetch_stream(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource> {
        let state = match self.streams.entry(key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                entry.insert(LocalStreamState::Pending {
                    subscribers: vec![],
                })
            }
        };
        match state {
            LocalStreamState::Created(stream) => stream.subscribe(),
            LocalStreamState::Pending { subscribers } => {
                let (sender, receiver) = oneshot::channel();
                subscribers.push(sender);
                Ok(Box::pin(
                    futures::stream::once(async move {
                        receiver
                            .await
                            .map_err(|error| TaskStreamError::External(Arc::new(error)))?
                            .map_err(|error| TaskStreamError::External(Arc::new(error)))
                    })
                    .try_flatten(),
                ))
            }
            LocalStreamState::Failed => Err(Self::creation_timeout()),
        }
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        self.streams
            .retain(|key, _| key.job_id != job_id || stage.is_some_and(|stage| key.stage != stage));
    }

    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey) {
        if let Some(state) = self.streams.get_mut(key)
            && let LocalStreamState::Pending { subscribers } = state
        {
            for subscriber in subscribers.drain(..) {
                let _ = subscriber.send(Err(Self::creation_timeout()));
            }
            *state = LocalStreamState::Failed;
        }
    }

    fn creation_timeout() -> ExecutionError {
        ExecutionError::InternalError("local stream is not created within the expected time".into())
    }
}
