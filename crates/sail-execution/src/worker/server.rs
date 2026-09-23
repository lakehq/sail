use std::sync::Arc;

use datafusion::execution::TaskContext;
use log::{debug, info};
use prost::Message;
use sail_common::actor::ActorHandle;
use tonic::{Request, Response, Status};

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskAttempt, TaskKey};
use crate::task::definition::TaskDefinition;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};
use crate::worker::r#gen::worker_service_server::WorkerService;
use crate::worker::r#gen::{
    CleanUpJobRequest, CleanUpJobResponse, RunTaskBatchRequest, RunTaskBatchResponse,
    StopTaskRequest, StopTaskResponse, StopWorkerRequest, StopWorkerResponse,
};
use crate::worker::{WorkerActor, WorkerMessage};

pub struct WorkerServer {
    worker: ActorHandle<WorkerActor>,
    task_runner: ActorHandle<TaskRunnerActor>,
    context: Arc<TaskContext>,
}

impl WorkerServer {
    pub fn new(
        worker: ActorHandle<WorkerActor>,
        task_runner: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
    ) -> Self {
        Self {
            worker,
            task_runner,
            context,
        }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServer {
    async fn run_task_batch(
        &self,
        request: Request<RunTaskBatchRequest>,
    ) -> Result<Response<RunTaskBatchResponse>, Status> {
        let request = request.into_inner();
        let started = std::time::Instant::now();
        debug!("{request:?}");
        let RunTaskBatchRequest {
            job_id,
            stage,
            tasks,
            definition,
            peers,
        } = request;
        info!(
            "RPC run_task_batch job={job_id} stage={stage} tasks={} peers={} definition_bytes={}",
            tasks.len(),
            peers.len(),
            definition.len()
        );
        let peers = peers
            .into_iter()
            .map(|x| x.try_into())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let definition = crate::task::r#gen::TaskDefinition::decode(definition.as_slice())
            .map_err(|e| Status::invalid_argument(format!("invalid task definition: {e}")))?;
        let stage =
            usize::try_from(stage).map_err(|_| Status::invalid_argument("stage overflow"))?;
        let tasks = tasks
            .into_iter()
            .map(|task| {
                Ok(TaskAttempt {
                    partition: usize::try_from(task.partition)
                        .map_err(|_| Status::invalid_argument("partition overflow"))?,
                    attempt: usize::try_from(task.attempt)
                        .map_err(|_| Status::invalid_argument("attempt overflow"))?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let (result, rx) = tokio::sync::oneshot::channel();
        self.task_runner
            .send(TaskRunnerMessage::RunTaskBatch {
                job_id: job_id.into(),
                stage,
                tasks,
                definition: Arc::new(TaskDefinition::try_from(definition)?),
                context: self.context.clone(),
                peers,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        rx.await
            .map_err(|_| Status::unavailable("task runner stopped before admission"))??;
        let response = RunTaskBatchResponse {};
        info!("RPC run_task_batch completed after {:?}", started.elapsed());
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn stop_task(
        &self,
        request: Request<StopTaskRequest>,
    ) -> Result<Response<StopTaskResponse>, Status> {
        let request = request.into_inner();
        let started = std::time::Instant::now();
        debug!("{request:?}");
        let StopTaskRequest {
            job_id,
            stage,
            partition,
            attempt,
        } = request;
        info!("RPC stop_task job={job_id} stage={stage} partition={partition} attempt={attempt}");
        self.task_runner
            .send(TaskRunnerMessage::StopTask {
                key: TaskKey {
                    job_id: job_id.into(),
                    stage: stage as usize,
                    partition: partition as usize,
                    attempt: attempt as usize,
                },
            })
            .await
            .map_err(ExecutionError::from)?;
        let response = StopTaskResponse {};
        info!("RPC stop_task completed after {:?}", started.elapsed());
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn clean_up_job(
        &self,
        request: Request<CleanUpJobRequest>,
    ) -> Result<Response<CleanUpJobResponse>, Status> {
        let request = request.into_inner();
        let started = std::time::Instant::now();
        debug!("{request:?}");
        let CleanUpJobRequest { job_id, stage } = request;
        info!("RPC clean_up_job job={job_id} stage={stage:?}");
        let job_id = job_id.into();
        let stage = stage.map(|x| x as usize);
        if stage.is_none() {
            self.task_runner
                .send(TaskRunnerMessage::CloseJob { job_id })
                .await
                .map_err(ExecutionError::from)?;
        }
        self.task_runner
            .send(TaskRunnerMessage::CleanUpLocalStreams { job_id, stage })
            .await
            .map_err(ExecutionError::from)?;
        self.task_runner
            .send(TaskRunnerMessage::CleanUpCelebornStreams { job_id, stage })
            .await
            .map_err(ExecutionError::from)?;
        let response = CleanUpJobResponse {};
        info!("RPC clean_up_job completed after {:?}", started.elapsed());
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn stop_worker(
        &self,
        request: Request<StopWorkerRequest>,
    ) -> Result<Response<StopWorkerResponse>, Status> {
        let request = request.into_inner();
        let started = std::time::Instant::now();
        debug!("{request:?}");
        let StopWorkerRequest {} = request;
        info!("RPC stop_worker");
        self.worker
            .send(WorkerMessage::Shutdown)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopWorkerResponse {};
        info!("RPC stop_worker completed after {:?}", started.elapsed());
        debug!("{response:?}");
        Ok(Response::new(response))
    }
}
