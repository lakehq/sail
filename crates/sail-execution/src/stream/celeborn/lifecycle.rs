use std::collections::HashMap;

use sail_celeborn::error::{CelebornError, CelebornResult};
use sail_celeborn::lifecycle::LifecycleManager;
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

#[tonic::async_trait]
impl LifecycleManager for RemoteLifecycleManager {
    async fn create_shuffle_id(&self, task_key: String) -> CelebornResult<i32> {
        self.client
            .create_shuffle_id(task_key)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn request_slots(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation> {
        let response = self
            .client
            .request_slots(shuffle_id, partition_ids, should_replicate, max_workers)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))?;
        let primary_locations = response
            .primary_locations
            .into_iter()
            .map(|location| {
                let location = PartitionLocation {
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
                    replicate_port: u16::try_from(location.replicate_port).map_err(|_| {
                        CelebornError::Protocol("invalid replication port".to_string())
                    })?,
                    peer: None,
                };
                Ok((location.id, location))
            })
            .collect::<CelebornResult<HashMap<_, _>>>()?;
        Ok(SlotReservation {
            worker_ids: vec![],
            primary_locations,
            worker_locations: HashMap::new(),
        })
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
