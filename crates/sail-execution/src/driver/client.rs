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
use crate::rpc::{ClientHandle, ClientOptions};

#[derive(Clone)]
pub struct DriverClient {
    worker_id: WorkerId,
    inner: ClientHandle<DriverServiceClient<Channel>>,
}

impl DriverClient {
    pub fn new(worker_id: WorkerId, options: ClientOptions) -> Self {
        Self {
            worker_id,
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
        status: TaskStatus,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(ReportTaskStatusRequest {
            worker_id: self.worker_id.into(),
            task_id: task_id.into(),
            status: gen::TaskStatus::from(status) as i32,
        });
        let response = self.inner.lock().await?.report_task_status(request).await?;
        let ReportTaskStatusResponse {} = response.into_inner();
        Ok(())
    }
}
