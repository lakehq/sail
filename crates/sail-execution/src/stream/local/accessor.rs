use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use futures::future::try_join_all;
use sail_common::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::driver::DriverMessage;
use crate::error::ExecutionResult;
use crate::id::{TaskKey, TaskStreamKey, WorkerId};
use crate::stream::accessor::MultiChannelTaskStreamSink;
use crate::stream::merge::merged_stream;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink, TaskStreamWriter};
use crate::task::definition::{TaskInput, TaskInputKey, TaskInputLocator};
use crate::worker::{WorkerMessage, WorkerStreamOwner};

/// Actor-specific operations required for local task streams.
pub trait LocalStreamAccessorMessage {
    fn create_stream(
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self;

    fn fetch_driver_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self;

    fn fetch_worker_stream(
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self;
}

impl LocalStreamAccessorMessage for DriverMessage {
    fn create_stream(
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self {
        Self::CreateLocalStream {
            key,
            replicas,
            schema,
            result,
        }
    }

    fn fetch_driver_stream(
        key: TaskStreamKey,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchDriverStream { key, result }
    }

    fn fetch_worker_stream(
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchWorkerStream {
            worker_id,
            key,
            schema,
            result,
        }
    }
}

impl LocalStreamAccessorMessage for WorkerMessage {
    fn create_stream(
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    ) -> Self {
        Self::CreateLocalStream {
            key,
            replicas,
            schema,
            result,
        }
    }

    fn fetch_driver_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchDriverStream {
            key,
            schema,
            result,
        }
    }

    fn fetch_worker_stream(
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        Self::FetchWorkerStream {
            owner: WorkerStreamOwner::Worker { worker_id, schema },
            key,
            result,
        }
    }
}

struct LocalStreamAccessor<T: Actor> {
    handle: ActorHandle<T>,
}

impl<T: Actor> LocalStreamAccessor<T> {
    fn new(handle: ActorHandle<T>) -> Self {
        Self { handle }
    }
}

impl<T: Actor> LocalStreamAccessor<T>
where
    T::Message: LocalStreamAccessorMessage,
{
    async fn receive<R>(
        &self,
        message: T::Message,
        rx: oneshot::Receiver<ExecutionResult<R>>,
    ) -> Result<R> {
        self.handle.send(message).await.map_err(|_| {
            DataFusionError::Internal("actor send error for local stream accessor".to_string())
        })?;
        rx.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn create_stream(
        &self,
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamChannelSink>> {
        let (tx, rx) = oneshot::channel();
        self.receive(T::Message::create_stream(key, replicas, schema, tx), rx)
            .await
    }

    async fn fetch_driver_stream(
        &self,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (tx, rx) = oneshot::channel();
        self.receive(T::Message::fetch_driver_stream(key, schema, tx), rx)
            .await
    }

    async fn fetch_worker_stream(
        &self,
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (tx, rx) = oneshot::channel();
        self.receive(
            T::Message::fetch_worker_stream(worker_id, key, schema, tx),
            rx,
        )
        .await
    }
}

pub(crate) struct LocalTaskStreamReader<T: Actor> {
    local: LocalStreamAccessor<T>,
    key: TaskKey,
    input: TaskInput,
    schema: SchemaRef,
}

impl<T: Actor> LocalTaskStreamReader<T> {
    pub(crate) fn new(
        handle: ActorHandle<T>,
        key: TaskKey,
        input: TaskInput,
        schema: SchemaRef,
    ) -> Self {
        Self {
            local: LocalStreamAccessor::new(handle),
            key,
            input,
            schema,
        }
    }
}

impl<T: Actor> fmt::Debug for LocalTaskStreamReader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalTaskStreamReader")
            .field("key", &self.key)
            .finish()
    }
}

impl<T: Actor> LocalTaskStreamReader<T>
where
    T::Message: LocalStreamAccessorMessage,
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
impl<T: Actor> TaskStreamReader for LocalTaskStreamReader<T>
where
    T::Message: LocalStreamAccessorMessage,
{
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let streams = match &self.input.locator {
            TaskInputLocator::Driver { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|key| {
                    self.local
                        .fetch_driver_stream(self.stream_key(key), self.schema.clone())
                }))
                .await?
            }
            TaskInputLocator::Worker { keys } => {
                let keys = keys.get(partition).ok_or_else(|| {
                    DataFusionError::Execution(format!("input partition {partition} not found"))
                })?;
                try_join_all(keys.iter().map(|(worker_id, key)| {
                    self.local.fetch_worker_stream(
                        *worker_id,
                        self.stream_key(key),
                        self.schema.clone(),
                    )
                }))
                .await?
            }
            TaskInputLocator::Storage { .. } => {
                return Err(DataFusionError::Internal(
                    "storage input assigned to local stream reader".to_string(),
                ));
            }
        };
        Ok(merged_stream(self.schema.clone(), streams))
    }
}

pub(crate) struct LocalTaskStreamWriter<T: Actor> {
    local: LocalStreamAccessor<T>,
    key: TaskKey,
    channels: usize,
    replicas: usize,
    schema: SchemaRef,
}

impl<T: Actor> LocalTaskStreamWriter<T> {
    pub(crate) fn new(
        handle: ActorHandle<T>,
        key: TaskKey,
        channels: usize,
        replicas: usize,
        schema: SchemaRef,
    ) -> Self {
        Self {
            local: LocalStreamAccessor::new(handle),
            key,
            channels,
            replicas,
            schema,
        }
    }
}

impl<T: Actor> fmt::Debug for LocalTaskStreamWriter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalTaskStreamWriter")
            .field("key", &self.key)
            .finish()
    }
}

impl<T: Actor> LocalTaskStreamWriter<T>
where
    T::Message: LocalStreamAccessorMessage,
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
impl<T: Actor> TaskStreamWriter for LocalTaskStreamWriter<T>
where
    T::Message: LocalStreamAccessorMessage,
{
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        if partition != self.key.partition {
            return Err(DataFusionError::Execution(format!(
                "task stream writer for partition {} cannot open partition {partition}",
                self.key.partition
            )));
        }
        let sinks = try_join_all((0..self.channels).map(|channel| {
            self.local.create_stream(
                self.stream_key(partition, channel),
                self.replicas,
                self.schema.clone(),
            )
        }))
        .await?;
        Ok(Box::new(MultiChannelTaskStreamSink {
            sinks: sinks.into_iter().map(Some).collect(),
        }))
    }
}
