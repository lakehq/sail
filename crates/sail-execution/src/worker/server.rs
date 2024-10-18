use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::worker::actor::WorkerActor;
use crate::worker::gen::worker_service_server::WorkerService;
use crate::worker::gen::{
    RunTaskRequest, RunTaskResponse, StopTaskRequest, StopTaskResponse, StopWorkerRequest,
    StopWorkerResponse,
};
use crate::worker::WorkerEvent;

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
    async fn run_task(
        &self,
        request: Request<RunTaskRequest>,
    ) -> Result<Response<RunTaskResponse>, Status> {
        let RunTaskRequest {
            task_id,
            partition,
            plan,
        } = request.into_inner();
        self.handle
            .send(WorkerEvent::RunTask {
                task_id: task_id.into(),
                partition: partition as usize,
                plan,
            })
            .await?;
        Ok(Response::new(RunTaskResponse {}))
    }

    async fn stop_task(
        &self,
        request: Request<StopTaskRequest>,
    ) -> Result<Response<StopTaskResponse>, Status> {
        let StopTaskRequest { task_id, partition } = request.into_inner();
        self.handle
            .send(WorkerEvent::StopTask {
                task_id: task_id.into(),
                partition: partition as usize,
            })
            .await?;
        Ok(Response::new(StopTaskResponse {}))
    }

    async fn stop_worker(
        &self,
        request: Request<StopWorkerRequest>,
    ) -> Result<Response<StopWorkerResponse>, Status> {
        let StopWorkerRequest {} = request.into_inner();
        self.handle.send(WorkerEvent::Shutdown).await?;
        Ok(Response::new(StopWorkerResponse {}))
    }
}
