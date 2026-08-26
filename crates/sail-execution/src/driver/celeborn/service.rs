use std::fmt::Display;
use std::sync::Arc;

use sail_celeborn::common::{ApplicationMetrics, PartitionLocation};
use sail_celeborn::lifecycle::{LifecycleManagerActor, LifecycleManagerMessage, ReviveRequest};
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::DriverRegistryAccessor;
use crate::driver::r#gen::celeborn_lifecycle_manager_service_server::CelebornLifecycleManagerService;
use crate::driver::r#gen::{
    CelebornGetJobShuffleIdsRequest, CelebornGetJobShuffleIdsResponse, CelebornGetShuffleIdRequest,
    CelebornGetShuffleIdResponse, CelebornMapperEndRequest, CelebornMapperEndResponse,
    CelebornRegisterShuffleRequest, CelebornRegisterShuffleResponse, CelebornReportMetricsRequest,
    CelebornReportMetricsResponse, CelebornReviveRequest, CelebornReviveResponse,
    CelebornShuffleId, CelebornUnregisterShuffleRequest, CelebornUnregisterShuffleResponse,
};
use crate::error::ExecutionError;
use crate::id::DriverId;

pub(crate) struct CelebornLifecycleManagerServer {
    registry: Arc<dyn DriverRegistryAccessor>,
}

impl CelebornLifecycleManagerServer {
    pub(crate) fn new(registry: Arc<dyn DriverRegistryAccessor>) -> Self {
        Self { registry }
    }

    async fn celeborn_lifecycle_manager(
        &self,
        driver_id: DriverId,
    ) -> Result<ActorHandle<LifecycleManagerActor>, Status> {
        let manager = self
            .registry
            .get(driver_id)
            .await?
            .celeborn_lifecycle_manager()
            .await
            .map_err(status)?;
        manager
            .ok_or_else(|| Status::failed_precondition("Celeborn lifecycle manager is not enabled"))
    }
}

fn status(error: impl Display) -> Status {
    Status::internal(error.to_string())
}

#[tonic::async_trait]
impl CelebornLifecycleManagerService for CelebornLifecycleManagerServer {
    async fn get_shuffle_id(
        &self,
        request: Request<CelebornGetShuffleIdRequest>,
    ) -> Result<Response<CelebornGetShuffleIdResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::GetShuffleId {
                job_id: request.job_id,
                stage: request.stage,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let shuffle_id = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornGetShuffleIdResponse { shuffle_id }))
    }

    async fn get_job_shuffle_ids(
        &self,
        request: Request<CelebornGetJobShuffleIdsRequest>,
    ) -> Result<Response<CelebornGetJobShuffleIdsResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::GetJobShuffleIds {
                job_id: request.job_id,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let shuffle_ids = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornGetJobShuffleIdsResponse {
            shuffle_ids: shuffle_ids
                .into_iter()
                .map(|(stage, shuffle_id)| CelebornShuffleId { stage, shuffle_id })
                .collect(),
        }))
    }

    async fn register_shuffle(
        &self,
        request: Request<CelebornRegisterShuffleRequest>,
    ) -> Result<Response<CelebornRegisterShuffleResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::RegisterShuffle {
                shuffle_id: request.shuffle_id,
                partition_ids: request.partition_ids,
                should_replicate: request.should_replicate,
                max_workers: request.max_workers,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let reservation = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornRegisterShuffleResponse {
            primary_locations: reservation
                .primary_locations
                .into_values()
                .map(Into::into)
                .collect(),
            all_primary_locations: reservation
                .worker_locations
                .into_values()
                .flat_map(|locations| locations.primary_locations)
                .map(Into::into)
                .collect(),
        }))
    }

    async fn revive(
        &self,
        request: Request<CelebornReviveRequest>,
    ) -> Result<Response<CelebornReviveResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let old_location: PartitionLocation = request
            .old_location
            .ok_or_else(|| Status::invalid_argument("missing old Celeborn partition location"))?
            .try_into()
            .map_err(status)?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::Revive {
                request: ReviveRequest {
                    shuffle_id: request.shuffle_id,
                    partition_id: request.partition_id,
                    map_id: request.map_id,
                    attempt_id: request.attempt_id,
                    old_location,
                    cause: request.cause,
                },
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let new_location = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornReviveResponse {
            location: Some(new_location.into()),
        }))
    }

    async fn mapper_end(
        &self,
        request: Request<CelebornMapperEndRequest>,
    ) -> Result<Response<CelebornMapperEndResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::MapperEnd {
                shuffle_id: request.shuffle_id,
                map_id: request.map_id,
                attempt_id: request.attempt_id,
                num_mappers: request.num_mappers,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornMapperEndResponse {}))
    }

    async fn unregister_shuffle(
        &self,
        request: Request<CelebornUnregisterShuffleRequest>,
    ) -> Result<Response<CelebornUnregisterShuffleResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::UnregisterShuffle {
                shuffle_id: request.shuffle_id,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornUnregisterShuffleResponse {}))
    }

    async fn report_metrics(
        &self,
        request: Request<CelebornReportMetricsRequest>,
    ) -> Result<Response<CelebornReportMetricsResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::ReportMetrics {
                metrics: ApplicationMetrics {
                    total_written: request.total_written,
                    file_count: request.file_count,
                    ..Default::default()
                },
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornReportMetricsResponse {}))
    }
}
