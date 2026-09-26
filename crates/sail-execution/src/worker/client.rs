use std::num::NonZeroUsize;
use std::sync::Arc;

use prost::Message;

use crate::error::ExecutionResult;
use crate::id::{JobId, TaskAttempt, TaskKey};
use crate::rpc::{ClientHandle, ClientOptions, ClientService};
use crate::stream::service::{TaskStreamFlightClient, TaskStreamOwner};
use crate::task::definition::TaskDefinition;
use crate::worker::WorkerLocation;
use crate::worker::r#gen::worker_service_client::WorkerServiceClient;
use crate::worker::r#gen::{
    CleanUpJobRequest, CleanUpJobResponse, RunTaskBatchRequest, RunTaskBatchResponse,
    StopTaskRequest, StopTaskResponse, StopWorkerRequest, StopWorkerResponse,
};

#[derive(Clone)]
pub struct WorkerClientSet {
    pub core: WorkerClient,
    pub flight: TaskStreamFlightClient,
}

impl WorkerClientSet {
    pub fn new(options: ClientOptions, flight_connection_count: NonZeroUsize) -> Self {
        Self {
            core: WorkerClient::new(options.clone()),
            flight: TaskStreamFlightClient::new(
                options,
                TaskStreamOwner::Worker,
                flight_connection_count,
            ),
        }
    }
}

#[derive(Clone)]
pub struct WorkerClient {
    inner: ClientHandle<WorkerServiceClient<ClientService>>,
}

impl WorkerClient {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            inner: ClientHandle::new(options.clone()),
        }
    }
}

impl WorkerClient {
    /// Sends a batch of task attempts from one stage to a remote worker via gRPC.
    pub async fn run_task_batch(
        &self,
        job_id: JobId,
        stage: usize,
        tasks: Vec<TaskAttempt>,
        definition: Arc<TaskDefinition>,
        peers: Vec<WorkerLocation>,
    ) -> ExecutionResult<()> {
        let definition =
            crate::task::r#gen::TaskDefinition::from(definition.as_ref().clone()).encode_to_vec();
        log::debug!(
            "task batch tasks={} encoded definition bytes={}",
            tasks.len(),
            definition.len()
        );
        let request = RunTaskBatchRequest {
            job_id: job_id.into(),
            stage: stage as u64,
            tasks: tasks
                .into_iter()
                .map(|task| crate::worker::r#gen::TaskAttempt {
                    partition: task.partition as u64,
                    attempt: task.attempt as u64,
                })
                .collect(),
            definition,
            peers: peers.into_iter().map(|x| x.into()).collect(),
        };
        let response = self.inner.get().await?.run_task_batch(request).await?;
        let RunTaskBatchResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn stop_task(&self, key: TaskKey) -> ExecutionResult<()> {
        let request = StopTaskRequest {
            job_id: key.job_id.into(),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
        };
        let response = self.inner.get().await?.stop_task(request).await?;
        let StopTaskResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn clean_up_job(&self, job_id: JobId, stage: Option<usize>) -> ExecutionResult<()> {
        let request = CleanUpJobRequest {
            job_id: job_id.into(),
            stage: stage.map(|x| x as u64),
        };
        let response = self.inner.get().await?.clean_up_job(request).await?;
        let CleanUpJobResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn stop_worker(&self) -> ExecutionResult<()> {
        let request = StopWorkerRequest {};
        let response = self.inner.get().await?.stop_worker(request).await?;
        let StopWorkerResponse {} = response.into_inner();
        Ok(())
    }
}
