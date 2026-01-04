mod core;

use std::fmt;

use datafusion::arrow::datatypes::SchemaRef;
use sail_server::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::driver::DriverEvent;
use crate::error::ExecutionResult;
use crate::id::{TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::worker::{WorkerEvent, WorkerStreamOwner};

pub struct StreamAccessor<T: Actor> {
    handle: ActorHandle<T>,
}

impl<T: Actor> fmt::Debug for StreamAccessor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamAccessor").finish()
    }
}

pub trait StreamAccessorMessage {
    fn create_local_stream(
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> Self;

    fn create_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
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

    fn fetch_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self;
}

impl StreamAccessorMessage for DriverEvent {
    fn create_local_stream(
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> Self {
        DriverEvent::CreateLocalStream {
            key,
            storage,
            schema,
            result,
        }
    }

    fn create_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> Self {
        DriverEvent::CreateRemoteStream {
            uri,
            key,
            schema,
            result,
        }
    }

    fn fetch_driver_stream(
        key: TaskStreamKey,
        _schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        DriverEvent::FetchDriverStream { key, result }
    }

    fn fetch_worker_stream(
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        DriverEvent::FetchWorkerStream {
            worker_id,
            key,
            schema,
            result,
        }
    }

    fn fetch_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        DriverEvent::FetchRemoteStream {
            uri,
            key,
            schema,
            result,
        }
    }
}

impl StreamAccessorMessage for WorkerEvent {
    fn create_local_stream(
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> Self {
        WorkerEvent::CreateLocalStream {
            key,
            storage,
            schema,
            result,
        }
    }

    fn create_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> Self {
        WorkerEvent::CreateRemoteStream {
            uri,
            key,
            schema,
            result,
        }
    }

    fn fetch_driver_stream(
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        WorkerEvent::FetchDriverStream {
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
        WorkerEvent::FetchWorkerStream {
            owner: WorkerStreamOwner::Worker { worker_id, schema },
            key,
            result,
        }
    }

    fn fetch_remote_stream(
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> Self {
        WorkerEvent::FetchRemoteStream {
            uri,
            key,
            schema,
            result,
        }
    }
}
