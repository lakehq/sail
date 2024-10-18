use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::driver::actor::DriverActor;
use crate::driver::gen::driver_service_server::DriverService;
use crate::driver::gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse,
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
        let RegisterWorkerRequest {
            worker_id,
            host,
            port,
        } = request.into_inner();
        let port = u16::try_from(port).map_err(|_| {
            Status::invalid_argument("port must be a valid 16-bit unsigned integer")
        })?;
        let event = DriverEvent::RegisterWorker {
            worker_id: WorkerId::from(worker_id),
            host,
            port,
        };
        self.handle.send(event).await?;
        Ok(Response::new(RegisterWorkerResponse {}))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let ReportTaskStatusRequest {
            task_id,
            partition,
            status,
        } = request.into_inner();
        let status = gen::TaskStatus::try_from(status)
            .map_err(ExecutionError::from)?
            .try_into()?;
        let event = DriverEvent::TaskUpdated {
            task_id: task_id.into(),
            partition: partition as usize,
            status,
        };
        self.handle.send(event).await?;
        Ok(Response::new(ReportTaskStatusResponse {}))
    }
}
