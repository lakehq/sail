use std::sync::Arc;

use log::debug;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use prost::Message;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::r#gen::driver_service_server::DriverService;
use crate::driver::r#gen::{
    RegisterWorkerRequest, RegisterWorkerResponse, ReportMetricsRequest, ReportMetricsResponse,
    ReportTaskStatusRequest, ReportTaskStatusResponse, ReportWorkerHeartbeatRequest,
    ReportWorkerHeartbeatResponse, ReportWorkerKnownPeersRequest, ReportWorkerKnownPeersResponse,
};
use crate::driver::{DriverMessage, DriverRegistryAccessor, r#gen};
use crate::error::ExecutionError;
use crate::id::{DriverId, TaskKey, WorkerId};
use crate::profiling::ProfileHandle;

macro_rules! rpc {
    ($profile:expr, $operation:literal, $phase:literal, $duration:expr, $($detail:tt)*) => {
        if let Some(profile) = &$profile {
            profile.rpc("driver", $operation, $phase, || format!($($detail)*), $duration);
        }
    };
}

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
        let profile = ProfileHandle::for_driver(driver_id);
        let started = profile.as_ref().map(|_| std::time::Instant::now());
        rpc!(
            profile,
            "register_worker",
            "start",
            None,
            "driver={driver_id} worker={worker_id} host={host} port={port}"
        );
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
        rpc!(
            profile,
            "register_worker",
            "complete",
            started.map(|x| x.elapsed().as_micros()),
            "completed"
        );
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
        let profile = ProfileHandle::for_driver(driver_id);
        let started = profile.as_ref().map(|_| std::time::Instant::now());
        rpc!(
            profile,
            "report_worker_heartbeat",
            "start",
            None,
            "driver={driver_id} worker={worker_id}"
        );
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
        rpc!(
            profile,
            "report_worker_heartbeat",
            "complete",
            started.map(|x| x.elapsed().as_micros()),
            "completed"
        );
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
        let profile = ProfileHandle::for_driver(driver_id);
        let started = profile.as_ref().map(|_| std::time::Instant::now());
        rpc!(
            profile,
            "report_worker_known_peers",
            "start",
            None,
            "driver={driver_id} worker={worker_id} peers={peer_worker_ids:?}"
        );
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
        rpc!(
            profile,
            "report_worker_known_peers",
            "complete",
            started.map(|x| x.elapsed().as_micros()),
            "completed"
        );
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
        let profile = ProfileHandle::for_driver(driver_id);
        let started = profile.as_ref().map(|_| std::time::Instant::now());
        rpc!(
            profile,
            "report_task_status",
            "start",
            None,
            "driver={driver_id} job={job_id} stage={stage} partition={partition} attempt={attempt} status={status} sequence={sequence}"
        );
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
        rpc!(
            profile,
            "report_task_status",
            "complete",
            started.map(|x| x.elapsed().as_micros()),
            "completed"
        );
        debug!("{response:?}");
        Ok(Response::new(response))
    }

    async fn report_metrics(
        &self,
        request: Request<ReportMetricsRequest>,
    ) -> Result<Response<ReportMetricsResponse>, Status> {
        let ReportMetricsRequest { driver_id, metrics } = request.into_inner();
        let profile = ProfileHandle::for_driver(driver_id);
        let started = profile.as_ref().map(|_| std::time::Instant::now());
        rpc!(
            profile,
            "report_metrics",
            "start",
            None,
            "driver={driver_id} resources={}",
            metrics.len()
        );
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
        rpc!(
            profile,
            "report_metrics",
            "complete",
            started.map(|x| x.elapsed().as_micros()),
            "completed"
        );
        Ok(Response::new(ReportMetricsResponse {}))
    }
}
