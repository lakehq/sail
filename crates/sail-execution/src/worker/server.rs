use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::worker::actor::WorkerActor;
use crate::worker::rpc::worker_service_server::WorkerService;
use crate::worker::rpc::{
    LaunchTaskRequest, LaunchTaskResponse, StopWorkerRequest, StopWorkerResponse,
};

pub struct WorkerServer {
    handle: ActorHandle<WorkerActor>,
}

impl WorkerServer {
    pub fn new(handle: ActorHandle<WorkerActor>) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServer {
    async fn launch_task(
        &self,
        request: Request<LaunchTaskRequest>,
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
