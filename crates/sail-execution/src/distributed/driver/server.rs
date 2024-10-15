use tonic::{Request, Response, Status};

use crate::rpc::driver_service_server::DriverService;
use crate::rpc::{RegisterWorkerRequest, RegisterWorkerResponse};

pub(crate) struct DriverServer {}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_worker(
        &self,
        _request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        todo!()
    }
}
