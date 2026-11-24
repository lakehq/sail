// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use log::debug;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use prost::Message;
use sail_object_store::{StorageCredentialsError, StorageCredentialsRequest};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::r#gen::driver_service_server::DriverService;
use crate::driver::r#gen::{
    GetStorageCredentialsRequest, GetStorageCredentialsResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportMetricsRequest, ReportMetricsResponse, ReportTaskStatusRequest,
    ReportTaskStatusResponse, ReportWorkerHeartbeatRequest, ReportWorkerHeartbeatResponse,
    ReportWorkerKnownPeersRequest, ReportWorkerKnownPeersResponse,
};
use crate::driver::{DriverMessage, DriverRegistryAccessor, r#gen};
use crate::error::ExecutionError;
use crate::id::{DriverId, TaskKey, WorkerId};

pub struct DriverServer {
    registry: Arc<dyn DriverRegistryAccessor>,
}

impl DriverServer {
    pub fn new(registry: Arc<dyn DriverRegistryAccessor>) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl DriverService for DriverServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let RegisterWorkerRequest {
            driver_id,
            worker_id,
            host,
            port,
        } = request;
        let port = u16::try_from(port).map_err(|_| {
            Status::invalid_argument("port must be a valid 16-bit unsigned integer")
        })?;
        let (tx, rx) = oneshot::channel();
        let message = DriverMessage::RegisterWorker {
            worker_id: WorkerId::from(worker_id),
            host,
            port,
            result: tx,
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(message)
            .await
            .map_err(ExecutionError::from)?;
        rx.await.map_err(ExecutionError::from)??;
        let response = RegisterWorkerResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_worker_heartbeat(
        &self,
        request: Request<ReportWorkerHeartbeatRequest>,
    ) -> Result<Response<ReportWorkerHeartbeatResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportWorkerHeartbeatRequest {
            driver_id,
            worker_id,
        } = request;
        let message = DriverMessage::WorkerHeartbeat {
            worker_id: worker_id.into(),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(message)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportWorkerHeartbeatResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_worker_known_peers(
        &self,
        request: Request<ReportWorkerKnownPeersRequest>,
    ) -> Result<Response<ReportWorkerKnownPeersResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportWorkerKnownPeersRequest {
            driver_id,
            worker_id,
            peer_worker_ids,
        } = request;
        let message = DriverMessage::WorkerKnownPeers {
            worker_id: worker_id.into(),
            peer_worker_ids: peer_worker_ids.into_iter().map(|x| x.into()).collect(),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(message)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportWorkerKnownPeersResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_task_status(
        &self,
        request: Request<ReportTaskStatusRequest>,
    ) -> Result<Response<ReportTaskStatusResponse>, Status> {
        let request = request.into_inner();
        debug!("{request:?}");
        let ReportTaskStatusRequest {
            driver_id,
            job_id,
            stage,
            partition,
            attempt,
            status,
            message,
            cause,
            sequence,
        } = request;
        let status = r#gen::TaskStatus::try_from(status).map_err(ExecutionError::from)?;
        let cause = cause
            .map(|x| serde_json::from_str(&x))
            .transpose()
            .map_err(ExecutionError::from)?;
        let message = DriverMessage::UpdateTask {
            key: TaskKey {
                job_id: job_id.into(),
                stage: stage as usize,
                partition: partition as usize,
                attempt: attempt as usize,
            },
            status: status.into(),
            message,
            cause,
            sequence: Some(sequence),
        };
        self.registry
            .get(DriverId::from(driver_id))
            .await?
            .send(message)
            .await
            .map_err(ExecutionError::from)?;
        let response = ReportTaskStatusResponse {};
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn get_storage_credentials(
        &self,
        request: Request<GetStorageCredentialsRequest>,
    ) -> Result<Response<GetStorageCredentialsResponse>, Status> {
        // The request and the response are not logged.
        let GetStorageCredentialsRequest {
            driver_id,
            token,
            owner,
            location,
            access,
        } = request.into_inner();
        let access = r#gen::StorageAccess::try_from(access).map_err(ExecutionError::from)?;
        let stores = self
            .registry
            .get(DriverId::from(driver_id))
            .await?
            .vended_object_stores()
            .await?
            .ok_or_else(|| Status::permission_denied("the request is not accepted"))?;
        let request = StorageCredentialsRequest {
            token: Arc::from(token),
            owner,
            location,
            access: access.into(),
        };
        match stores.credentials(&request).await {
            Ok(credentials) => Ok(Response::new(credentials.into())),
            Err(StorageCredentialsError::Denied) => {
                Err(Status::permission_denied("the request is not accepted"))
            }
            Err(StorageCredentialsError::Unavailable(message)) => {
                Err(Status::failed_precondition(message))
            }
        }
    }

    async fn report_metrics(
        &self,
        request: Request<ReportMetricsRequest>,
    ) -> Result<Response<ReportMetricsResponse>, Status> {
        let ReportMetricsRequest { driver_id, metrics } = request.into_inner();
        // Validate that the destination driver still exists before accepting worker metrics.
        self.registry.get(DriverId::from(driver_id)).await?;
        let reporter = sail_telemetry::telemetry::global_system_metric_reporter()
            .ok_or_else(|| Status::failed_precondition("metrics event store is not initialized"))?;
        let metrics = metrics
            .into_iter()
            .map(|data| {
                ResourceMetrics::decode(data.as_slice()).map_err(|error| {
                    Status::invalid_argument(format!("invalid OpenTelemetry metrics data: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        reporter
            .report(metrics)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(ReportMetricsResponse {}))
    }
}
