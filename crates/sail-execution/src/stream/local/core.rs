use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
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
use crate::stream::local::shared::{ClosedSink, SharedStream};
use crate::stream::local::{LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
            options,
            streams: HashMap::new(),
            removed: HashMap::new(),
        }
    }

    pub fn create_stream(
        &mut self,
        key: TaskStreamKey,
        replicas: usize,
        shared: Option<Arc<TaskContext>>,
        schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        if self
            .removed
            .get(&(key.job_id, key.stage))
            .and_then(|attempts| attempts.get(key.partition))
            .is_some_and(|attempt| key.attempt <= *attempt)
        {
            // Every consumer of the stream has finished, so the stream is not read.
            return Ok(Box::new(ClosedSink));
        }
        let create = |senders: Vec<_>| -> ExecutionResult<_> {
            match &shared {
                Some(context) => {
                    let (stream, sink) = SharedStream::try_new(replicas, senders, context, schema)?;
                    Ok((LocalStreamState::Shared { stream }, sink))
                }
                None => {
                    let mut stream =
                        Self::create_stream_with_senders(replicas, senders, &self.options);
                    let sink = stream.publish()?;
                    Ok((LocalStreamState::Created { stream }, sink))
                }
            }
        };

        match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let senders = match entry.get_mut() {
                    LocalStreamState::Created { .. } | LocalStreamState::Shared { .. } => {
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
                    Ok((state, sink)) => {
                        *entry.into_mut() = state;
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
                Ok((state, sink)) => {
                    entry.insert(state);
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
                LocalStreamState::Shared { stream } => stream.subscribe(),
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

    /// Removes the streams of a job, or the streams of a stage of the job up to the given
    /// attempt of each partition.
    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>, attempts: &[usize]) {
        if let Some(stage) = stage {
            self.streams.retain(|key, _| {
                key.job_id != job_id
                    || key.stage != stage
                    || attempts.get(key.partition).is_none_or(|a| key.attempt > *a)
            });
            let removed = self.removed.entry((job_id, stage)).or_default();
            if removed.len() < attempts.len() {
                removed.resize(attempts.len(), 0);
            }
            for (removed, attempt) in removed.iter_mut().zip(attempts) {
                *removed = (*removed).max(*attempt);
            }
        } else {
            self.streams.retain(|key, _| key.job_id != job_id);
            self.removed.retain(|(job, _), _| *job != job_id);
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use datafusion::arrow::datatypes::Schema;

    use super::*;
    use crate::stream::writer::TaskStreamWriteState;

    fn key(stage: usize) -> TaskStreamKey {
        attempt(stage, 0, 0)
    }

    fn attempt(stage: usize, partition: usize, attempt: usize) -> TaskStreamKey {
        TaskStreamKey {
            job_id: JobId::from(1),
            stage,
            partition,
            attempt,
            channel: 0,
        }
    }

    #[tokio::test]
    async fn a_stream_created_after_its_stage_is_removed_is_not_read() {
        let mut streams =
            LocalStreamManager::new(LocalStreamManagerOptions::new(1, Duration::from_secs(1)));
        let schema = Arc::new(Schema::empty());
        let context = Arc::new(TaskContext::default());
        // The task of partition 0 has run a second attempt when the stage is removed.
        streams.remove_streams(JobId::from(1), Some(0), &[1, 0]);
        for shared in [None, Some(context)] {
            let mut sink = streams
                .create_stream(key(0), 1, shared, Arc::clone(&schema))
                .unwrap();
            assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Closed);
        }
        let mut sink = streams
            .create_stream(attempt(0, 0, 1), 1, None, Arc::clone(&schema))
            .unwrap();
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Closed);
        // The streams of other stages are created as usual.
        let mut sink = streams
            .create_stream(key(1), 1, None, Arc::clone(&schema))
            .unwrap();
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
        // A later attempt of a partition, which a retry of its region runs, is read.
        let mut sink = streams
            .create_stream(attempt(0, 1, 1), 1, None, Arc::clone(&schema))
            .unwrap();
        assert_eq!(sink.ready().await.unwrap(), TaskStreamWriteState::Active);
        // A late clean-up of the earlier attempt keeps the stream of the later attempt.
        streams.remove_streams(JobId::from(1), Some(0), &[1, 0]);
        assert!(streams.streams.contains_key(&attempt(0, 1, 1)));
        // The stages are forgotten when the job is removed.
        streams.remove_streams(JobId::from(1), None, &[]);
        assert!(streams.removed.is_empty());
        assert!(streams.streams.is_empty());
    }
}
