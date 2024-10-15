use tonic::{Request, Response, Status};

use crate::rpc::worker_service_server::WorkerService;
use crate::rpc::{LaunchTaskRequest, LaunchTaskResponse, StopWorkerRequest, StopWorkerResponse};

#[derive(Debug)]
pub(crate) struct WorkerServer {}

impl WorkerServer {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServer {
    async fn launch_task(
        &self,
        _request: Request<LaunchTaskRequest>,
    ) -> Result<Response<LaunchTaskResponse>, Status> {
        todo!()
    }

    async fn stop_worker(
        &self,
        _request: Request<StopWorkerRequest>,
    ) -> Result<Response<StopWorkerResponse>, Status> {
        todo!()
    }
}
