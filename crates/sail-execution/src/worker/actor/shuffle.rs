use arrow::datatypes::SchemaRef;
use datafusion::common::{not_impl_err, DataFusionError, Result};
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
pub(super) struct WorkerTaskStreamReader {
    worker_id: WorkerId,
    handle: ActorHandle<WorkerActor>,
}

impl WorkerTaskStreamReader {
    pub fn new(worker_id: WorkerId, handle: ActorHandle<WorkerActor>) -> Self {
        Self { worker_id, handle }
    }
}

#[tonic::async_trait]
impl TaskStreamReader for WorkerTaskStreamReader {
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
                    WorkerEvent::FetchThisWorkerTaskStream {
                        channel: channel.clone(),
                        result: tx,
                    }
                } else {
                    WorkerEvent::FetchOtherWorkerTaskStream {
                        worker_id: *worker_id,
                        host: host.clone(),
                        port: *port,
                        channel: channel.clone(),
                        schema,
                        result: tx,
                    }
                }
            }
            TaskReadLocation::Remote { .. } => return not_impl_err!("remote task read"),
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

#[derive(Debug)]
pub(super) struct WorkerTaskStreamWriter {
    #[allow(dead_code)]
    worker_id: WorkerId,
    handle: ActorHandle<WorkerActor>,
}

impl WorkerTaskStreamWriter {
    pub fn new(worker_id: WorkerId, handle: ActorHandle<WorkerActor>) -> Self {
        Self { worker_id, handle }
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for WorkerTaskStreamWriter {
    async fn open(
        &self,
        location: &TaskWriteLocation,
        schema: SchemaRef,
    ) -> Result<Box<dyn RecordBatchStreamWriter>> {
        let (tx, rx) = oneshot::channel();
        let event = match location {
            TaskWriteLocation::Memory { channel } => WorkerEvent::CreateMemoryTaskStream {
                channel: channel.clone(),
                schema,
                result: tx,
            },
            TaskWriteLocation::Disk { .. } => {
                return not_impl_err!("disk task write");
            }
            TaskWriteLocation::Remote { .. } => {
                return not_impl_err!("remote task write");
            }
        };
        self.handle
            .send(event)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let sender = rx
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::new(sender))
    }
}
