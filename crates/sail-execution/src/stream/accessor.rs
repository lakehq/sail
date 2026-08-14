use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use futures::future::try_join_all;
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::merge::merged_stream;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{
    TaskStreamChannelSink, TaskStreamSink, TaskStreamWriteState, TaskStreamWriter,
};
use crate::task::definition::{TaskInput, TaskInputLocator, TaskOutput, TaskOutputLocator};
use crate::task_runner::{TaskRunnerActor, TaskRunnerExtensions, TaskRunnerMessage};

pub struct TaskStreamFactory<'a> {
    handle: ActorHandle<TaskRunnerActor>,
    context: Arc<TaskContext>,
    extensions: &'a TaskRunnerExtensions,
    mappers: usize,
}

impl Clone for TaskStreamFactory<'_> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            context: self.context.clone(),
            extensions: self.extensions,
            mappers: self.mappers,
        }
    }
}

impl<'a> TaskStreamFactory<'a> {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        extensions: &'a TaskRunnerExtensions,
        mappers: usize,
    ) -> Self {
        Self {
            handle,
            context,
            extensions,
            mappers,
        }
    }

    pub fn reader(
        &self,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamReader> {
        Arc::new(MultiChannelTaskStreamReader::new(
            self.handle.clone(),
            self.context.clone(),
            key,
            input,
            schema,
        ))
    }

    pub fn writer(
        &self,
        key: TaskKey,
        output: TaskOutput,
        schema: SchemaRef,
    ) -> Arc<dyn TaskStreamWriter> {
        if self.extensions.celeborn_streams.is_some()
            && matches!(output.locator, TaskOutputLocator::Blocking)
        {
            Arc::new(CelebornTaskStreamWriter::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                output.channels(),
                schema,
                self.mappers,
            ))
        } else {
            Arc::new(MultiChannelTaskStreamWriter::new(
                self.handle.clone(),
                self.context.clone(),
                key,
                output,
                schema,
            ))
        }
    }
}

struct TaskStreamAccessor {
    handle: ActorHandle<TaskRunnerActor>,
    context: Arc<TaskContext>,
}

impl TaskStreamAccessor {
    fn new(handle: ActorHandle<TaskRunnerActor>, context: Arc<TaskContext>) -> Self {
        Self { handle, context }
    }

    async fn receive<R>(
        &self,
        message: TaskRunnerMessage,
        rx: oneshot::Receiver<ExecutionResult<R>>,
    ) -> Result<R> {
        self.handle.send(message).await.map_err(|_| {
            DataFusionError::Internal("actor send error for task stream accessor".to_string())
        })?;
        rx.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn create_local_stream(
        &self,
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamChannelSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateLocalStream {
                key,
                replicas,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn create_storage_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamChannelSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateStorageStream {
                key,
                schema,
                context: self.context.clone(),
                result,
            },
            rx,
        )
        .await
    }

    async fn create_celeborn_stream(
        &self,
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::CreateCelebornStream {
                key,
                mappers,
                channels,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_driver_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchDriverStream {
                key,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_worker_stream(
        &self,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchWorkerStream {
                worker_id,
                key,
                schema,
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_storage_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchStorageStream {
                key,
                schema,
                context: self.context.clone(),
                result,
            },
            rx,
        )
        .await
    }

    async fn fetch_celeborn_stream(
        &self,
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (result, rx) = oneshot::channel();
        self.receive(
            TaskRunnerMessage::FetchCelebornStream {
                job_id,
                stage,
                channels,
                schema,
                result,
            },
            rx,
        )
        .await
    }
}

pub(crate) struct MultiChannelTaskStreamReader {
    streams: TaskStreamAccessor,
    key: TaskKey,
    input: TaskInput,
    schema: SchemaRef,
}

impl MultiChannelTaskStreamReader {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            input,
            schema,
        }
    }
}

impl fmt::Debug for MultiChannelTaskStreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiChannelTaskStreamReader").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamReader for MultiChannelTaskStreamReader {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let streams = match &self.input.locator {
            TaskInputLocator::Driver { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|key| {
                    self.streams.fetch_driver_stream(
                        key.task_stream_key(self.key.job_id, self.input.stage),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::Worker { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|(worker_id, key)| {
                    self.streams.fetch_worker_stream(
                        *worker_id,
                        key.task_stream_key(self.key.job_id, self.input.stage),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::Storage { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|key| {
                    self.streams.fetch_storage_stream(
                        key.task_stream_key(self.key.job_id, self.input.stage),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::ShuffleService { channels } => {
                let channels = channels.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                vec![
                    self.streams
                        .fetch_celeborn_stream(
                            self.key.job_id,
                            self.input.stage,
                            channels.clone(),
                            self.schema.clone(),
                        )
                        .await?,
                ]
            }
        };
        Ok(merged_stream(self.schema.clone(), streams))
    }
}

pub(crate) struct MultiChannelTaskStreamWriter {
    streams: TaskStreamAccessor,
    key: TaskKey,
    output: TaskOutput,
    schema: SchemaRef,
}

impl MultiChannelTaskStreamWriter {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        output: TaskOutput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            output,
            schema,
        }
    }
}

impl fmt::Debug for MultiChannelTaskStreamWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiChannelTaskStreamWriter").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for MultiChannelTaskStreamWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        let channels = self.output.channels();
        let sinks = match &self.output.locator {
            TaskOutputLocator::Pipelined { replicas } => {
                try_join_all((0..channels).map(|channel| {
                    self.streams.create_local_stream(
                        self.key.task_stream_key(channel),
                        *replicas,
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskOutputLocator::Blocking => {
                try_join_all((0..channels).map(|channel| {
                    self.streams.create_storage_stream(
                        self.key.task_stream_key(channel),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
        };
        Ok(Box::new(MultiChannelTaskStreamSink {
            sinks: sinks.into_iter().map(Some).collect(),
        }))
    }
}

pub(crate) struct CelebornTaskStreamWriter {
    streams: TaskStreamAccessor,
    key: TaskKey,
    channels: usize,
    schema: SchemaRef,
    mappers: usize,
}

impl CelebornTaskStreamWriter {
    fn new(
        handle: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        key: TaskKey,
        channels: usize,
        schema: SchemaRef,
        mappers: usize,
    ) -> Self {
        Self {
            streams: TaskStreamAccessor::new(handle, context),
            key,
            channels,
            schema,
            mappers,
        }
    }
}

impl fmt::Debug for CelebornTaskStreamWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelebornTaskStreamWriter").finish()
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for CelebornTaskStreamWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        self.streams
            .create_celeborn_stream(
                self.key.clone(),
                self.mappers,
                self.channels,
                self.schema.clone(),
            )
            .await
    }
}

pub(crate) struct MultiChannelTaskStreamSink {
    pub(crate) sinks: Vec<Option<Box<dyn TaskStreamChannelSink>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MultiChannelTaskStreamSink {
    async fn write(&mut self, channel: usize, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        let state = match self.sinks.get_mut(channel).ok_or_else(|| {
            DataFusionError::Execution(format!("shuffle output channel {channel} not found"))
        })? {
            Some(sink) => sink.write(batch).await?,
            None => TaskStreamWriteState::Closed,
        };
        if state == TaskStreamWriteState::Closed {
            self.sinks[channel] = None;
        }
        Ok(if self.sinks.iter().any(Option::is_some) {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        for sink in self.sinks.into_iter().flatten() {
            sink.commit().await?;
        }
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        for sink in self.sinks.into_iter().flatten() {
            sink.abort().await?;
        }
        Ok(())
    }
}
