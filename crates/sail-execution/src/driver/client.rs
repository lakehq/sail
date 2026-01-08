use sail_common_datafusion::error::CommonErrorCause;

use crate::driver::event::TaskStatus;
use crate::driver::gen;
use crate::driver::gen::driver_service_client::DriverServiceClient;
use crate::driver::gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse,
};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskId, WorkerId};
use crate::rpc::{ClientHandle, ClientOptions, ClientService};

#[derive(Clone)]
pub struct DriverClient {
    inner: ClientHandle<DriverServiceClient<ClientService>>,
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
        let response = self.inner.get().await?.register_worker(request).await?;
        let RegisterWorkerResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_worker_heartbeat(&self, worker_id: WorkerId) -> ExecutionResult<()> {
        let request = tonic::Request::new(gen::ReportWorkerHeartbeatRequest {
            worker_id: worker_id.into(),
        });
        let response = self
            .inner
            .get()
            .await?
            .report_worker_heartbeat(request)
            .await?;
        let gen::ReportWorkerHeartbeatResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_worker_known_peers(
        &self,
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ExecutionResult<()> {
        let request = tonic::Request::new(gen::ReportWorkerKnownPeersRequest {
            worker_id: worker_id.into(),
            peer_worker_ids: peer_worker_ids.into_iter().map(|id| id.into()).collect(),
        });
        let response = self
            .inner
            .get()
            .await?
            .report_worker_known_peers(request)
            .await?;
        let gen::ReportWorkerKnownPeersResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_task_status(
        &self,
        job_id: JobId,
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        sequence: u64,
    ) -> ExecutionResult<()> {
        let cause = cause
            .map(|x| {
                serde_json::to_string(&x).map_err(|e| {
                    ExecutionError::InternalError(format!("failed to serialize cause: {e}"))
                })
            })
            .transpose()?;
        let request = tonic::Request::new(ReportTaskStatusRequest {
            job_id: job_id.into(),
            task_id: task_id.into(),
            attempt: attempt as u64,
            status: gen::TaskStatus::from(status) as i32,
            message,
            cause,
            sequence,
        });
        let response = self.inner.get().await?.report_task_status(request).await?;
        let ReportTaskStatusResponse {} = response.into_inner();
        Ok(())
    }
}
