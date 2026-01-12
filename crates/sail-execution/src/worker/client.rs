use prost::Message;

use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey};
use crate::rpc::{ClientHandle, ClientOptions, ClientService};
use crate::stream_service::TaskStreamFlightClient;
use crate::task::definition::TaskDefinition;
use crate::worker::event::WorkerLocation;
use crate::worker::gen::worker_service_client::WorkerServiceClient;
use crate::worker::gen::{
    CleanUpJobRequest, CleanUpJobResponse, RunTaskRequest, RunTaskResponse, StopTaskRequest,
    StopTaskResponse, StopWorkerRequest, StopWorkerResponse,
};

#[derive(Clone)]
pub struct WorkerClientSet {
    pub core: WorkerClient,
    pub flight: TaskStreamFlightClient,
}

impl WorkerClientSet {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            core: WorkerClient::new(options.clone()),
            flight: TaskStreamFlightClient::new(options),
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
    pub async fn run_task(
        &self,
        key: TaskKey,
        definition: TaskDefinition,
        peers: Vec<WorkerLocation>,
    ) -> ExecutionResult<()> {
        let definition = crate::task::gen::TaskDefinition::from(definition).encode_to_vec();
        let request = RunTaskRequest {
            job_id: key.job_id.into(),
            stage: key.stage as u64,
            attempt: key.attempt as u64,
            partition: key.partition as u64,
            definition,
            peers: peers.into_iter().map(|x| x.into()).collect(),
        };
        let response = self.inner.get().await?.run_task(request).await?;
        let RunTaskResponse {} = response.into_inner();
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
