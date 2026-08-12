use std::collections::HashMap;

use sail_celeborn::error::{CelebornError, CelebornResult};
use sail_celeborn::lifecycle::{LifecycleManager, ReviveRequest};
use sail_celeborn::master::{PartitionLocation, SlotReservation};

use crate::driver::CelebornLifecycleManagerClient;

#[derive(Clone)]
pub(crate) struct RemoteLifecycleManager {
    client: CelebornLifecycleManagerClient,
}

impl RemoteLifecycleManager {
    pub(crate) fn new(client: CelebornLifecycleManagerClient) -> Self {
        Self { client }
    }
}

fn to_proto(location: PartitionLocation) -> crate::driver::r#gen::CelebornPartitionLocation {
    crate::driver::r#gen::CelebornPartitionLocation {
        id: location.id,
        epoch: location.epoch,
        host: location.host,
        rpc_port: u32::from(location.rpc_port),
        push_port: u32::from(location.push_port),
        fetch_port: u32::from(location.fetch_port),
        replicate_port: u32::from(location.replicate_port),
        peer: location.peer.map(|peer| Box::new(to_proto(*peer))),
        mode: location.mode,
    }
}

fn from_proto(
    location: crate::driver::r#gen::CelebornPartitionLocation,
) -> CelebornResult<PartitionLocation> {
    Ok(PartitionLocation {
        mode: location.mode,
        id: location.id,
        epoch: location.epoch,
        host: location.host,
        rpc_port: u16::try_from(location.rpc_port)
            .map_err(|_| CelebornError::Protocol("invalid RPC port".to_string()))?,
        push_port: u16::try_from(location.push_port)
            .map_err(|_| CelebornError::Protocol("invalid push port".to_string()))?,
        fetch_port: u16::try_from(location.fetch_port)
            .map_err(|_| CelebornError::Protocol("invalid fetch port".to_string()))?,
        replicate_port: u16::try_from(location.replicate_port)
            .map_err(|_| CelebornError::Protocol("invalid replication port".to_string()))?,
        peer: location
            .peer
            .map(|peer| from_proto(*peer).map(Box::new))
            .transpose()?,
    })
}

#[tonic::async_trait]
impl LifecycleManager for RemoteLifecycleManager {
    async fn get_shuffle_id(&self, job_id: u64, stage: u64) -> CelebornResult<i32> {
        self.client
            .get_shuffle_id(job_id, stage)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn get_job_shuffle_ids(&self, job_id: u64) -> CelebornResult<Vec<(u64, i32)>> {
        self.client
            .get_job_shuffle_ids(job_id)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation> {
        let response = self
            .client
            .register_shuffle(shuffle_id, partition_ids, should_replicate, max_workers)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))?;
        let primary_locations = response
            .primary_locations
            .into_iter()
            .map(|location| {
                let location = from_proto(location)?;
                Ok((location.id, location))
            })
            .collect::<CelebornResult<HashMap<_, _>>>()?;
        let mut worker_locations =
            HashMap::<String, sail_celeborn::master::WorkerSlotLocations>::new();
        for location in response.all_primary_locations {
            let location = from_proto(location)?;
            let worker_id = format!(
                "{}:{}:{}:{}:{}",
                location.host,
                location.rpc_port,
                location.push_port,
                location.fetch_port,
                location.replicate_port,
            );
            worker_locations
                .entry(worker_id)
                .or_insert_with(|| sail_celeborn::master::WorkerSlotLocations {
                    primary_locations: Vec::new(),
                    replica_locations: Vec::new(),
                })
                .primary_locations
                .push(location);
        }
        Ok(SlotReservation {
            worker_ids: vec![],
            primary_locations,
            worker_locations,
        })
    }

    async fn revive(&self, request: ReviveRequest) -> CelebornResult<PartitionLocation> {
        let response = self
            .client
            .revive(
                request.shuffle_id,
                request.partition_id,
                request.map_id,
                request.attempt_id,
                to_proto(request.old_location),
                request.cause,
            )
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))?;
        from_proto(response.location.ok_or_else(|| {
            CelebornError::Protocol("missing revived partition location".to_string())
        })?)
    }

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> CelebornResult<()> {
        self.client
            .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()> {
        self.client
            .unregister_shuffle(shuffle_id)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn stop(&self) -> CelebornResult<()> {
        Ok(())
    }
}
