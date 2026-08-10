use sail_common_datafusion::error::CommonErrorCause;
use tonic::Request;

use crate::driver::r#gen::celeborn_lifecycle_manager_service_client::CelebornLifecycleManagerServiceClient;
use crate::driver::r#gen::driver_service_client::DriverServiceClient;
use crate::driver::r#gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse,
};
use crate::driver::{TaskStatus, r#gen};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{DriverId, TaskKey, WorkerId};
use crate::rpc::{ClientHandle, ClientOptions, ClientService};
use crate::stream::service::{TaskStreamFlightClient, TaskStreamOwner};

#[derive(Clone)]
pub struct DriverClientSet {
    pub core: DriverClient,
    pub celeborn: CelebornLifecycleManagerClient,
    pub flight: TaskStreamFlightClient,
}

impl DriverClientSet {
    pub fn new(driver_id: DriverId, options: ClientOptions) -> Self {
        Self {
            core: DriverClient::new(driver_id, options.clone()),
            celeborn: CelebornLifecycleManagerClient::new(driver_id, options.clone()),
            flight: TaskStreamFlightClient::new(options, TaskStreamOwner::Driver { driver_id }),
        }
    }
}

#[derive(Clone)]
pub struct CelebornLifecycleManagerClient {
    inner: ClientHandle<CelebornLifecycleManagerServiceClient<ClientService>>,
    driver_id: DriverId,
}

impl CelebornLifecycleManagerClient {
    pub(crate) fn new(driver_id: DriverId, options: ClientOptions) -> Self {
        Self {
            inner: ClientHandle::new(options),
            driver_id,
        }
    }

    pub async fn create_shuffle_id(&self, task_key: String) -> ExecutionResult<i32> {
        Ok(self
            .inner
            .get()
            .await?
            .create_shuffle_id(Request::new(r#gen::CelebornCreateShuffleIdRequest {
                driver_id: self.driver_id.into(),
                task_key,
            }))
            .await?
            .into_inner()
            .shuffle_id)
    }

    pub async fn request_slots(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> ExecutionResult<r#gen::CelebornRequestSlotsResponse> {
        Ok(self
            .inner
            .get()
            .await?
            .request_slots(Request::new(r#gen::CelebornRequestSlotsRequest {
                driver_id: self.driver_id.into(),
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
            }))
            .await?
            .into_inner())
    }

    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> ExecutionResult<()> {
        self.inner
            .get()
            .await?
            .mapper_end(Request::new(r#gen::CelebornMapperEndRequest {
                driver_id: self.driver_id.into(),
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
            }))
            .await?;
        Ok(())
    }

    pub async fn unregister_shuffle(&self, shuffle_id: i32) -> ExecutionResult<()> {
        self.inner
            .get()
            .await?
            .unregister_shuffle(Request::new(r#gen::CelebornUnregisterShuffleRequest {
                driver_id: self.driver_id.into(),
                shuffle_id,
            }))
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct DriverClient {
    inner: ClientHandle<DriverServiceClient<ClientService>>,
    driver_id: DriverId,
}

impl DriverClient {
    pub fn new(driver_id: DriverId, options: ClientOptions) -> Self {
        Self {
            inner: ClientHandle::new(options.clone()),
            driver_id,
        }
    }

    pub async fn register_worker(
        &self,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<()> {
        let request = Request::new(RegisterWorkerRequest {
            driver_id: self.driver_id.into(),
            worker_id: worker_id.into(),
            host,
            port: port as u32,
        });
        let response = self.inner.get().await?.register_worker(request).await?;
        let RegisterWorkerResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_worker_heartbeat(&self, worker_id: WorkerId) -> ExecutionResult<()> {
        let request = Request::new(r#gen::ReportWorkerHeartbeatRequest {
            driver_id: self.driver_id.into(),
            worker_id: worker_id.into(),
        });
        let response = self
            .inner
            .get()
            .await?
            .report_worker_heartbeat(request)
            .await?;
        let r#gen::ReportWorkerHeartbeatResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_worker_known_peers(
        &self,
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ExecutionResult<()> {
        let request = Request::new(r#gen::ReportWorkerKnownPeersRequest {
            driver_id: self.driver_id.into(),
            worker_id: worker_id.into(),
            peer_worker_ids: peer_worker_ids.into_iter().map(|id| id.into()).collect(),
        });
        let response = self
            .inner
            .get()
            .await?
            .report_worker_known_peers(request)
            .await?;
        let r#gen::ReportWorkerKnownPeersResponse {} = response.into_inner();
        Ok(())
    }

    pub async fn report_task_status(
        &self,
        key: TaskKey,
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
        let request = Request::new(ReportTaskStatusRequest {
            driver_id: self.driver_id.into(),
            job_id: key.job_id.into(),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
            status: r#gen::TaskStatus::from(status) as i32,
            message,
            cause,
            sequence,
        });
        let response = self.inner.get().await?.report_task_status(request).await?;
        let ReportTaskStatusResponse {} = response.into_inner();
        Ok(())
    }
}
