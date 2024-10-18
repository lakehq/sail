use tonic::transport::Channel;

use crate::driver::gen;
use crate::driver::gen::driver_service_client::DriverServiceClient;
use crate::driver::gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse,
};
use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{TaskId, WorkerId};
use crate::rpc::{ClientBuilder, ClientHandle, ClientOptions};

#[derive(Clone)]
pub struct DriverClient {
    inner: ClientHandle<DriverServiceClient<Channel>>,
}

#[tonic::async_trait]
impl ClientBuilder for DriverServiceClient<Channel> {
    async fn connect(options: &ClientOptions) -> ExecutionResult<Self> {
        Ok(DriverServiceClient::connect(options.to_url_string()).await?)
    }
}

impl DriverClient {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            inner: ClientHandle::new(options),
        }
    }

    pub async fn register_worker(
        &self,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(RegisterWorkerRequest {
            worker_id: worker_id.into(),
            host,
            port: port as u32,
        });
        let response = self.inner.lock().await?.register_worker(request).await?;
        let RegisterWorkerResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_task_status(
        &self,
        task_id: TaskId,
        partition: usize,
        status: TaskStatus,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(ReportTaskStatusRequest {
            task_id: task_id.into(),
            partition: partition as u64,
            status: gen::TaskStatus::from(status) as i32,
        });
        let response = self.inner.lock().await?.report_task_status(request).await?;
        let ReportTaskStatusResponse {} = response.into_inner();
        Ok(())
    }
}
