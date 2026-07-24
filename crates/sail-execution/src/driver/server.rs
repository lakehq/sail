use std::sync::Arc;

use log::debug;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::r#gen::driver_service_server::DriverService;
use crate::driver::r#gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse, ReportWorkerHeartbeatRequest, ReportWorkerHeartbeatResponse,
    ReportWorkerKnownPeersRequest, ReportWorkerKnownPeersResponse,
};
use crate::driver::{DriverEvent, DriverRegistryAccessor, r#gen};
use crate::error::ExecutionError;
use crate::id::{DriverId, TaskKey, WorkerId};

pub struct DriverServer {
    registry: Arc<dyn DriverRegistryAccessor>,
}

impl DriverServer {
    pub fn new(registry: Arc<dyn DriverRegistryAccessor>) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let RegisterWorkerRequest {
            driver_id,
            worker_id,
            host,
            port,
        } = request;
        let port = u16::try_from(port).map_err(|_| {
            Status::invalid_argument("port must be a valid 16-bit unsigned integer")
        })?;
        let (tx, rx) = oneshot::channel();
        let event = DriverEvent::RegisterWorker {
            worker_id: WorkerId::from(worker_id),
            host,
            port,
            result: tx,
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        rx.await.map_err(ExecutionError::from)??;
        let response = RegisterWorkerResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_worker_heartbeat(
        &self,
        request: Request<ReportWorkerHeartbeatRequest>,
    ) -> Result<Response<ReportWorkerHeartbeatResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportWorkerHeartbeatRequest {
            driver_id,
            worker_id,
        } = request;
        let event = DriverEvent::WorkerHeartbeat {
            worker_id: worker_id.into(),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportWorkerHeartbeatResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_worker_known_peers(
        &self,
        request: Request<ReportWorkerKnownPeersRequest>,
    ) -> Result<Response<ReportWorkerKnownPeersResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportWorkerKnownPeersRequest {
            driver_id,
            worker_id,
            peer_worker_ids,
        } = request;
        let event = DriverEvent::WorkerKnownPeers {
            worker_id: worker_id.into(),
            peer_worker_ids: peer_worker_ids.into_iter().map(|x| x.into()).collect(),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportWorkerKnownPeersResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportTaskStatusRequest {
            driver_id,
            job_id,
            stage,
            partition,
            attempt,
            status,
            message,
            cause,
            sequence,
        } = request;
        let status = r#gen::TaskStatus::try_from(status).map_err(ExecutionError::from)?;
        let cause = cause
            .map(|x| serde_json::from_str(&x))
            .transpose()
            .map_err(ExecutionError::from)?;
        let event = DriverEvent::UpdateTask {
            key: TaskKey {
                job_id: job_id.into(),
                stage: stage as usize,
                partition: partition as usize,
                attempt: attempt as usize,
            },
            status: status.into(),
            message,
            cause,
            sequence: Some(sequence),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportTaskStatusResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }
}
