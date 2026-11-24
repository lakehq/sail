// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::TaskContext;
use log::debug;
use prost::Message;
use sail_common::actor::ActorHandle;
use sail_object_store::{SessionStorageCredentialFetch, StorageRoutes, VendedObjectStores};
use tonic::{Request, Response, Status};

use crate::driver::DriverClient;
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
    /// Gets the storage credentials of the session from the driver.
    storage_credentials: SessionStorageCredentialFetch,
}

impl WorkerServer {
    pub fn new(
        worker: ActorHandle<WorkerActor>,
        task_runner: ActorHandle<TaskRunnerActor>,
        context: Arc<TaskContext>,
        driver: DriverClient,
    ) -> Self {
        let storage_credentials: SessionStorageCredentialFetch = Arc::new(move |request| {
            let driver = driver.clone();
            Box::pin(async move {
                driver
                    .get_storage_credentials(request)
                    .await
                    .map_err(|e| match e {
                        // The message of the driver is not a secret.
                        ExecutionError::TonicStatusError(status) => format!(
                            "failed to get storage credentials from the driver: {}",
                            status.message()
                        ),
                        e => format!("failed to get storage credentials from the driver: {e}"),
                    })
            })
        });
        Self {
            worker,
            task_runner,
            context,
            storage_credentials,
        }
    }

    /// Accesses the storage locations in the same way as the session of the driver.
    fn apply_storage_routes(&self, routes: StorageRoutes) -> ExecutionResult<()> {
        let stores = self
            .context
            .session_config()
            .get_extension::<VendedObjectStores>()
            .ok_or_else(|| {
                ExecutionError::InternalError(
                    "the worker session cannot use storage credentials issued by catalogs"
                        .to_string(),
                )
            })?;
        stores.apply_routes(
            self.context.runtime_env().object_store_registry.as_ref(),
            &routes,
            &self.storage_credentials,
        )?;
        Ok(())
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServer {
    async fn run_task_batch(
        &self,
        request: Request<RunTaskBatchRequest>,
    ) -> Result<Response<RunTaskBatchResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let RunTaskBatchRequest {
            job_id,
            stage,
            tasks,
            definition,
            peers,
            storage_routes,
        } = request;
        // The routes are in place before the plan of the tasks is created.
        if let Some(routes) = storage_routes {
            self.apply_storage_routes(routes.into())?;
        }
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
        self.worker
            .send(WorkerMessage::Shutdown)
            .await
            .map_err(ExecutionError::from)?;
        let response = StopWorkerResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }
}
