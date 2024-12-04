use log::debug;
use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::error::ExecutionError;
use crate::worker::actor::WorkerActor;
use crate::worker::gen::worker_service_server::WorkerService;
use crate::worker::gen::{
    RemoveStreamRequest, RemoveStreamResponse, RunTaskRequest, RunTaskResponse, StopTaskRequest,
    StopTaskResponse, StopWorkerRequest, StopWorkerResponse,
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
        let request = request.into_inner();
        debug!("{:?}", request);
        let RunTaskRequest {
            task_id,
            attempt,
            partition,
            plan,
            channel,
        } = request;
        let event = WorkerEvent::RunTask {
            task_id: task_id.into(),
            attempt: attempt as usize,
            partition: partition as usize,
            plan,
            channel: channel.map(|x| x.into()),
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = RunTaskResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn stop_task(
        &self,
        request: Request<StopTaskRequest>,
    ) -> Result<Response<StopTaskResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let StopTaskRequest { task_id, attempt } = request;
        let event = WorkerEvent::StopTask {
            task_id: task_id.into(),
            attempt: attempt as usize,
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopTaskResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn remove_stream(
        &self,
        request: Request<RemoveStreamRequest>,
    ) -> Result<Response<RemoveStreamResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let RemoveStreamRequest { channel_prefix } = request;
        let event = WorkerEvent::RemoveLocalStream { channel_prefix };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = RemoveStreamResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }

    async fn stop_worker(
        &self,
        request: Request<StopWorkerRequest>,
    ) -> Result<Response<StopWorkerResponse>, Status> {
        let request = request.into_inner();
        debug!("{:?}", request);
        let StopWorkerRequest {} = request;
        self.handle
            .send(WorkerEvent::Shutdown)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopWorkerResponse {};
        debug!("{:?}", response);
        Ok(Response::new(response))
    }
}
