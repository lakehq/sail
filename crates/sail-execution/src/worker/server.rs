use log::debug;
use prost::Message;
use sail_server::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskKey;
use crate::task::definition::TaskDefinition;
use crate::worker::actor::WorkerActor;
use crate::worker::gen::worker_service_server::WorkerService;
use crate::worker::gen::{
    CleanUpJobRequest, CleanUpJobResponse, RunTaskRequest, RunTaskResponse, StopTaskRequest,
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
        debug!("{request:?}");
        let RunTaskRequest {
            job_id,
            stage,
            partition,
            attempt,
            definition,
            peers,
        } = request;
        let peers = peers
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let definition = crate::task::gen::TaskDefinition::decode(definition.as_slice())
            .map_err(|e| Status::invalid_argument(format!("invalid task definition: {e}")))?;
        let event = WorkerEvent::RunTask {
            key: TaskKey {
                job_id: job_id.into(),
                stage: stage as usize,
                partition: partition as usize,
                attempt: attempt as usize,
            },
            definition: TaskDefinition::try_from(definition)?,
            peers,
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = RunTaskResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn stop_task(
        &self,
        request: Request<StopTaskRequest>,
    ) -> Result<Response<StopTaskResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let StopTaskRequest {
            job_id,
            stage,
            partition,
            attempt,
        } = request;
        let event = WorkerEvent::StopTask {
            key: TaskKey {
                job_id: job_id.into(),
                stage: stage as usize,
                partition: partition as usize,
                attempt: attempt as usize,
            },
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopTaskResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn clean_up_job(
        &self,
        request: Request<CleanUpJobRequest>,
    ) -> Result<Response<CleanUpJobResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let CleanUpJobRequest { job_id, stage } = request;
        let event = WorkerEvent::CleanUpJob {
            job_id: job_id.into(),
            stage: stage.map(|x| x as usize),
        };
        self.handle
            .send(event)
            .await
            .map_err(ExecutionError::from)?;
        let response = CleanUpJobResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn stop_worker(
        &self,
        request: Request<StopWorkerRequest>,
    ) -> Result<Response<StopWorkerResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let StopWorkerRequest {} = request;
        self.handle
            .send(WorkerEvent::Shutdown)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopWorkerResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }
}
