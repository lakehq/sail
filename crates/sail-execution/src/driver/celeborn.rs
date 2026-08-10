use std::fmt::Display;
use std::sync::Arc;

use sail_celeborn::lifecycle::{LifecycleManagerActor, LifecycleManagerMessage};
use sail_celeborn::master::PartitionLocation;
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::driver::DriverRegistryAccessor;
use crate::driver::r#gen::celeborn_lifecycle_manager_service_server::CelebornLifecycleManagerService;
use crate::driver::r#gen::{
    CelebornCreateShuffleIdRequest, CelebornCreateShuffleIdResponse, CelebornMapperEndRequest,
    CelebornMapperEndResponse, CelebornPartitionLocation, CelebornRequestSlotsRequest,
    CelebornRequestSlotsResponse, CelebornUnregisterShuffleRequest,
    CelebornUnregisterShuffleResponse,
};
use crate::error::ExecutionError;
use crate::id::DriverId;

pub struct CelebornLifecycleManagerServer {
    registry: Arc<dyn DriverRegistryAccessor>,
}

impl CelebornLifecycleManagerServer {
    pub fn new(registry: Arc<dyn DriverRegistryAccessor>) -> Self {
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

fn location(partition: PartitionLocation) -> CelebornPartitionLocation {
    CelebornPartitionLocation {
        id: partition.id,
        epoch: partition.epoch,
        host: partition.host,
        rpc_port: partition.rpc_port as u32,
        push_port: partition.push_port as u32,
        fetch_port: partition.fetch_port as u32,
        replicate_port: partition.replicate_port as u32,
        peer: partition.peer.map(|peer| Box::new(location(*peer))),
        mode: partition.mode,
    }
}

#[tonic::async_trait]
impl CelebornLifecycleManagerService for CelebornLifecycleManagerServer {
    async fn create_shuffle_id(
        &self,
        request: Request<CelebornCreateShuffleIdRequest>,
    ) -> Result<Response<CelebornCreateShuffleIdResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::CreateShuffleId {
                shuffle_key: request.shuffle_key,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let shuffle_id = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornCreateShuffleIdResponse {
            shuffle_id,
        }))
    }

    async fn request_slots(
        &self,
        request: Request<CelebornRequestSlotsRequest>,
    ) -> Result<Response<CelebornRequestSlotsResponse>, Status> {
        let request = request.into_inner();
        let manager = self
            .celeborn_lifecycle_manager(DriverId::from(request.driver_id))
            .await?;
        let (result, receiver) = oneshot::channel();
        manager
            .send(LifecycleManagerMessage::RequestSlotsBegin {
                shuffle_id: request.shuffle_id,
                partition_ids: request.partition_ids,
                should_replicate: request.should_replicate,
                max_workers: request.max_workers,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        let reservation = receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornRequestSlotsResponse {
            primary_locations: reservation
                .primary_locations
                .into_values()
                .map(location)
                .collect(),
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
            .send(LifecycleManagerMessage::MapperEndBegin {
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
            .send(LifecycleManagerMessage::UnregisterShuffleBegin {
                shuffle_id: request.shuffle_id,
                result,
            })
            .await
            .map_err(ExecutionError::from)?;
        receiver.await.map_err(status)?.map_err(status)?;
        Ok(Response::new(CelebornUnregisterShuffleResponse {}))
    }
}
