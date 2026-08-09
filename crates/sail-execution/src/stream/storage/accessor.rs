use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use futures::future::try_join_all;
use sail_common::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::driver::DriverMessage;
use crate::error::ExecutionResult;
use crate::id::{TaskKey, TaskStreamKey};
use crate::stream::accessor::MultiChannelTaskStreamSink;
use crate::stream::merge::merged_stream;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink, TaskStreamWriter};
use crate::task::definition::{TaskInput, TaskInputKey, TaskInputLocator};
use crate::worker::WorkerMessage;

/// Actor-specific operations required for storage task streams.
pub trait StorageStreamAccessorMessage {
    fn create_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self;

    fn fetch_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self;
}

impl StorageStreamAccessorMessage for DriverMessage {
    fn create_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self {
        Self::CreateStorageStream {
            key,
            schema,
            context,
            result,
        }
    }

    fn fetch_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchStorageStream {
            key,
            schema,
            context,
            result,
        }
    }
}

impl StorageStreamAccessorMessage for WorkerMessage {
    fn create_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self {
        Self::CreateStorageStream {
            key,
            schema,
            context,
            result,
        }
    }

    fn fetch_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchStorageStream {
            key,
            schema,
            context,
            result,
        }
    }
}

struct StorageStreamAccessor<T: Actor> {
    handle: ActorHandle<T>,
    context: Arc<TaskContext>,
}

impl<T: Actor> StorageStreamAccessor<T> {
    fn new(handle: ActorHandle<T>, context: Arc<TaskContext>) -> Self {
        Self { handle, context }
    }
}

impl<T: Actor> StorageStreamAccessor<T>
where
    T::Message: StorageStreamAccessorMessage,
{
    async fn receive<R>(
        &self,
        message: T::Message,
        rx: oneshot::Receiver<ExecutionResult<R>>,
    ) -> Result<R> {
        self.handle.send(message).await.map_err(|_| {
            DataFusionError::Internal("actor send error for storage stream accessor".to_string())
        })?;
        rx.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn create_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamChannelSink>> {
        let (tx, rx) = oneshot::channel();
        self.receive(
            T::Message::create_stream(key, schema, self.context.clone(), tx),
            rx,
        )
        .await
    }

    async fn fetch_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (tx, rx) = oneshot::channel();
        self.receive(
            T::Message::fetch_stream(key, schema, self.context.clone(), tx),
            rx,
        )
        .await
    }
}

pub(crate) struct StorageTaskStreamReader<T: Actor> {
    storage: StorageStreamAccessor<T>,
    key: TaskKey,
    input: TaskInput,
    schema: SchemaRef,
}

impl<T: Actor> StorageTaskStreamReader<T> {
    pub(crate) fn new(
        handle: ActorHandle<T>,
        context: Arc<TaskContext>,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            storage: StorageStreamAccessor::new(handle, context),
            key,
            input,
            schema,
        }
    }
}

impl<T: Actor> fmt::Debug for StorageTaskStreamReader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageTaskStreamReader")
            .field("key", &self.key)
            .finish()
    }
}

impl<T: Actor> StorageTaskStreamReader<T>
where
    T::Message: StorageStreamAccessorMessage,
{
    fn stream_key(&self, key: &TaskInputKey) -> TaskStreamKey {
        TaskStreamKey {
            job_id: self.key.job_id,
            stage: self.input.stage,
            partition: key.partition,
            attempt: key.attempt,
            channel: key.channel,
        }
    }
}

#[tonic::async_trait]
impl<T: Actor> TaskStreamReader for StorageTaskStreamReader<T>
where
    T::Message: StorageStreamAccessorMessage,
{
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let TaskInputLocator::Storage { keys } = &self.input.locator else {
            return Err(DataFusionError::Internal(
                "non-storage input assigned to storage stream reader".to_string(),
            ));
        };
        let keys = keys.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!("input partition {partition} not found"))
        })?;
        let streams = try_join_all(keys.iter().map(|key| {
            self.storage
                .fetch_stream(self.stream_key(key), self.schema.clone())
        }))
        .await?;
        Ok(merged_stream(self.schema.clone(), streams))
    }
}

pub(crate) struct StorageTaskStreamWriter<T: Actor> {
    storage: StorageStreamAccessor<T>,
    key: TaskKey,
    channels: usize,
    schema: SchemaRef,
}

impl<T: Actor> StorageTaskStreamWriter<T> {
    pub(crate) fn new(
        handle: ActorHandle<T>,
        context: Arc<TaskContext>,
        key: TaskKey,
        channels: usize,
        schema: SchemaRef,
    ) -> Self {
        Self {
            storage: StorageStreamAccessor::new(handle, context),
            key,
            channels,
            schema,
        }
    }
}

impl<T: Actor> fmt::Debug for StorageTaskStreamWriter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageTaskStreamWriter")
            .field("key", &self.key)
            .finish()
    }
}

impl<T: Actor> StorageTaskStreamWriter<T>
where
    T::Message: StorageStreamAccessorMessage,
{
    fn stream_key(&self, partition: usize, channel: usize) -> TaskStreamKey {
        TaskStreamKey {
            job_id: self.key.job_id,
            stage: self.key.stage,
            partition,
            attempt: self.key.attempt,
            channel,
        }
    }
}

#[tonic::async_trait]
impl<T: Actor> TaskStreamWriter for StorageTaskStreamWriter<T>
where
    T::Message: StorageStreamAccessorMessage,
{
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        let sinks = try_join_all((0..self.channels).map(|channel| {
            self.storage
                .create_stream(self.stream_key(partition, channel), self.schema.clone())
        }))
        .await?;
        Ok(Box::new(MultiChannelTaskStreamSink {
            sinks: sinks.into_iter().map(Some).collect(),
        }))
    }
}
