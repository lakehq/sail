use tonic::transport::Channel;

use crate::error::ExecutionResult;
use crate::id::TaskId;
use crate::rpc::{ClientBuilder, ClientHandle, ClientOptions};
use crate::worker::gen::worker_service_client::WorkerServiceClient;
use crate::worker::gen::{RunTaskRequest, RunTaskResponse};

#[derive(Clone)]
pub struct WorkerClient {
    inner: ClientHandle<WorkerServiceClient<Channel>>,
}

impl WorkerClient {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            inner: ClientHandle::new(options),
        }
    }
}

#[tonic::async_trait]
impl ClientBuilder for WorkerServiceClient<Channel> {
    async fn connect(options: &ClientOptions) -> ExecutionResult<Self> {
        Ok(WorkerServiceClient::connect(options.to_url_string()).await?)
    }
}

impl WorkerClient {
    pub async fn run_task(
        &self,
        task_id: TaskId,
        partition: usize,
        plan: Vec<u8>,
    ) -> ExecutionResult<()> {
        let request = RunTaskRequest {
            task_id: task_id.into(),
            partition: partition as u64,
            plan,
        };
        let response = self.inner.lock().await?.run_task(request).await?;
        let RunTaskResponse {} = response.into_inner();
        Ok(())
    }
}
