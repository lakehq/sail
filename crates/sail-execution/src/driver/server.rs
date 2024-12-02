use log::debug;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::actor::DriverActor;
use crate::driver::gen::driver_service_server::DriverService;
use crate::driver::gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse, ReportWorkerHeartbeatRequest, ReportWorkerHeartbeatResponse,
};
use crate::driver::{gen, DriverEvent};
use crate::error::ExecutionError;
use crate::id::WorkerId;

pub struct DriverServer {
    handle: ActorHandle<DriverActor>,
}

impl DriverServer {
    pub fn new(handle: ActorHandle<DriverActor>) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let RegisterWorkerRequest {
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
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        rx.await.map_err(ExecutionError::from)??;
        let response = RegisterWorkerResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn report_worker_heartbeat(
        &self,
        request: Request<ReportWorkerHeartbeatRequest>,
    ) -> Result<Response<ReportWorkerHeartbeatResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let ReportWorkerHeartbeatRequest { worker_id } = request;
        let event = DriverEvent::WorkerHeartbeat {
            worker_id: worker_id.into(),
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportWorkerHeartbeatResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let ReportTaskStatusRequest {
            task_id,
            status,
            attempt,
            message,
            sequence,
        } = request;
        let status = gen::TaskStatus::try_from(status).map_err(ExecutionError::from)?;
        let event = DriverEvent::UpdateTask {
            task_id: task_id.into(),
            attempt: attempt as usize,
            status: status.into(),
            message,
            sequence: Some(sequence),
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportTaskStatusResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }
}
