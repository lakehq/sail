use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::id::WorkerId;
use crate::stream::reader::{TaskReadLocation, TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriter, TaskWriteLocation};
use crate::worker::{WorkerActor, WorkerEvent};

#[derive(Debug)]
pub(super) struct WorkerStreamAccessor {
    worker_id: WorkerId,
    handle: ActorHandle<WorkerActor>,
}

impl WorkerStreamAccessor {
    pub fn new(worker_id: WorkerId, handle: ActorHandle<WorkerActor>) -> Self {
        Self { worker_id, handle }
    }
}

#[tonic::async_trait]
impl TaskStreamReader for WorkerStreamAccessor {
    async fn open(
        &self,
        location: &TaskReadLocation,
        schema: SchemaRef,
    ) -> Result<TaskStreamSource> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskReadLocation::Driver { key } => WorkerEvent::FetchDriverStream {
                key: key.clone(),
                schema,
                result: tx,
            },
            TaskReadLocation::Worker { worker_id, key } => {
                if *worker_id == self.worker_id {
                    WorkerEvent::FetchThisWorkerStream {
                        key: key.clone(),
                        result: tx,
                    }
                } else {
                    WorkerEvent::FetchOtherWorkerStream {
                        worker_id: *worker_id,
                        key: key.clone(),
                        schema,
                        result: tx,
                    }
                }
            }
            TaskReadLocation::Remote { uri, key } => WorkerEvent::FetchRemoteStream {
                uri: uri.clone(),
                key: key.clone(),
                schema,
                result: tx,
            },
        };
        self.handle
            .send(event)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        rx.await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for WorkerStreamAccessor {
    async fn open(
        &self,
        location: &TaskWriteLocation,
        schema: SchemaRef,
    ) -> Result<Box<dyn TaskStreamSink>> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskWriteLocation::Local { key, storage } => WorkerEvent::CreateLocalStream {
                key: key.clone(),
                storage: *storage,
                schema,
                result: tx,
            },
            TaskWriteLocation::Remote { uri, key } => WorkerEvent::CreateRemoteStream {
                uri: uri.clone(),
                key: key.clone(),
                schema,
                result: tx,
            },
        };
        self.handle
            .send(event)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        rx.await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
