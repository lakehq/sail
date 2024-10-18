use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::driver::actor::DriverActor;
use crate::driver::rpc::driver_service_server::DriverService;
use crate::driver::rpc::{RegisterWorkerRequest, RegisterWorkerResponse};
use crate::driver::DriverEvent;
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
            id: WorkerId::from(worker_id),
            host,
            port,
        };
        self.handle.send(event).await?;
        Ok(Response::new(RegisterWorkerResponse {}))
    }
}
