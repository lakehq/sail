use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use log::warn;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorContext};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::stream_manager::local::{LocalStream, MemoryStream};
use crate::stream_manager::options::StreamManagerOptions;
use crate::stream_manager::remote::RemoteStreamManager;
use crate::stream_manager::{LocalStreamState, StreamManager, StreamManagerMessage};

impl StreamManager {
    pub fn new(options: StreamManagerOptions) -> Self {
        let remote_streams = match &options.shuffle {
            crate::shuffle::ShuffleServiceKind::None => None,
            crate::shuffle::ShuffleServiceKind::Storage {
                path,
                max_file_size,
                compression,
            } => Some(Arc::new(RemoteStreamManager::new(
                path.clone(),
                *max_file_size,
                *compression,
            ))),
        };
        Self {
            options,
            remote_streams,
            local_streams: HashMap::new(),
        }
    }

    pub fn create_local_stream(
        &mut self,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let create = |senders: Vec<_>| -> ExecutionResult<_> {
            let mut stream =
                Self::create_local_stream_with_senders(storage, senders, &self.options)?;
            let sink = stream.publish()?;
            Ok((stream, sink))
        };

        match self.local_streams.entry(key.clone()) {
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

    pub fn create_remote_stream(
        &mut self,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        context: &TaskContext,
    ) -> ExecutionResult<Box<dyn TaskStreamSink>> {
        let Some(remote_streams) = &self.remote_streams else {
            return Err(ExecutionError::InternalError(
                "remote stream requested without a storage shuffle service".to_string(),
            ));
        };
        remote_streams.create_stream(uri, key, schema, context)
    }

    pub fn fetch_local_stream<T>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource>
    where
        T: Actor,
        T::Message: StreamManagerMessage,
    {
        match self.local_streams.entry(key.clone()) {
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
                    T::Message::probe_pending_local_stream(key.clone()),
                    self.options.task_stream_creation_timeout,
                );
                Ok(Box::pin(ReceiverStream::new(rx)))
            }
        }
    }

    pub fn fetch_remote_stream<T>(
        &mut self,
        _ctx: &mut ActorContext<T>,
        uri: String,
        key: &TaskStreamKey,
        schema: SchemaRef,
        context: &TaskContext,
    ) -> ExecutionResult<TaskStreamSource>
    where
        T: Actor,
        T::Message: StreamManagerMessage,
    {
        let Some(remote_streams) = &self.remote_streams else {
            return Err(ExecutionError::InternalError(
                "remote stream requested without a storage shuffle service".to_string(),
            ));
        };
        remote_streams.fetch_stream(uri, key.clone(), schema, context)
    }

    pub fn remove_local_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        if let Some(stage) = stage {
            self.local_streams
                .retain(|key, _| key.job_id != job_id || key.stage != stage);
        } else {
            self.local_streams.retain(|key, _| key.job_id != job_id);
        }
    }

    pub fn remove_remote_streams<T>(
        &mut self,
        ctx: &mut ActorContext<T>,
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    ) where
        T: Actor,
    {
        let Some(remote_streams) = self.remote_streams.clone() else {
            return;
        };
        ctx.spawn(async move {
            if let Err(e) = remote_streams.remove_streams(job_id, stage, &context).await {
                warn!("failed to remove remote shuffle data for job {job_id}: {e}");
            }
        });
    }

    pub fn fail_local_stream_if_pending(&mut self, key: &TaskStreamKey) {
        let Some(value) = self.local_streams.get_mut(key) else {
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

    pub async fn stop(&mut self) {
        // TODO: remove all remote streams
    }

    fn create_local_stream_with_senders(
        storage: LocalStreamStorage,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        options: &StreamManagerOptions,
    ) -> ExecutionResult<Box<dyn LocalStream>> {
        match storage {
            LocalStreamStorage::Memory { replicas } => Ok(Box::new(MemoryStream::new(
                options.task_stream_buffer,
                replicas,
                senders,
            ))),
            LocalStreamStorage::Disk => Err(ExecutionError::InternalError(
                "not implemented: local disk storage".to_string(),
            )),
        }
    }
}
