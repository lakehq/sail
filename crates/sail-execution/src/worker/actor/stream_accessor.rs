use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::id::WorkerId;
use crate::stream::{
    RecordBatchStreamWriter, TaskReadLocation, TaskStreamReader, TaskStreamWriter,
    TaskWriteLocation,
};
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
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskReadLocation::Worker {
                worker_id,
                host,
                port,
                channel,
            } => {
                if *worker_id == self.worker_id {
                    WorkerEvent::FetchThisWorkerStream {
                        channel: channel.clone(),
                        result: tx,
                    }
                } else {
                    WorkerEvent::FetchOtherWorkerStream {
                        worker_id: *worker_id,
                        host: host.clone(),
                        port: *port,
                        channel: channel.clone(),
                        schema,
                        result: tx,
                    }
                }
            }
            TaskReadLocation::Remote { uri } => WorkerEvent::FetchRemoteStream {
                uri: uri.clone(),
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
    ) -> Result<Box<dyn RecordBatchStreamWriter>> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskWriteLocation::Local { channel, storage } => WorkerEvent::CreateLocalStream {
                channel: channel.clone(),
                storage: *storage,
                schema,
                result: tx,
            },
            TaskWriteLocation::Remote { uri } => WorkerEvent::CreateRemoteStream {
                uri: uri.clone(),
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
