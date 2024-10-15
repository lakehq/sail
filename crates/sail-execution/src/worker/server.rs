use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::worker::rpc::worker_service_server::WorkerService;
use crate::worker::rpc::{
    LaunchTaskRequest, LaunchTaskResponse, StopWorkerRequest, StopWorkerResponse,
};
use crate::worker::state::WorkerState;

pub(crate) struct WorkerServer {
    state: Arc<Mutex<WorkerState>>,
}

impl WorkerServer {
    pub(super) fn new(state: Arc<Mutex<WorkerState>>) -> Self {
        Self { state }
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
