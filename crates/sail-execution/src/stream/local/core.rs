use std::collections::HashMap;
use std::collections::hash_map::Entry;

use datafusion::execution::TaskContext;
use sail_common::actor::ActorContext;

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

    pub fn buffer(&self) -> usize {
        self.options.task_stream_buffer
    }

    pub fn create_stream(
        &mut self,
        key: TaskStreamKey,
        context: &TaskContext,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let state = self
            .streams
            .entry(key.clone())
            .or_insert_with(|| LocalStreamState {
                stream: MemoryStream::new(self.options.task_stream_buffer),
                pending: true,
            });
        if !state.pending {
            return Err(ExecutionError::InternalError(format!(
                "local stream {} is already created or failed",
                TaskStreamKeyDisplay(&key)
            )));
        }
        state.pending = false;
        Ok(Box::new(
            state
                .stream
                .publish(context.runtime_env().disk_manager.clone())?,
        ))
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
                entry.insert(LocalStreamState {
                    stream: MemoryStream::new(self.options.task_stream_buffer),
                    pending: true,
                })
            }
        };
        Ok(state.stream.subscribe())
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        self.streams
            .retain(|key, _| key.job_id != job_id || stage.is_some_and(|stage| key.stage != stage));
    }

    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey) {
        if let Some(state) = self.streams.get_mut(key)
            && state.pending
        {
            state.pending = false;
            state.stream.fail(TaskStreamError::Unknown(
                "local stream is not created within the expected time".into(),
            ));
        }
    }
}
