use std::collections::HashMap;

use sail_celeborn::common::{
    ApplicationMetrics, PartitionLocation, SlotReservation, WorkerIdentity, WorkerSlotLocations,
};
use sail_celeborn::error::{CelebornError, CelebornResult};
use sail_celeborn::lifecycle::{LifecycleManager, ReviveRequest};

use crate::driver::CelebornLifecycleManagerClient;
use crate::driver::r#gen::CelebornPartitionLocation;

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
                let location = PartitionLocation::try_from(location)?;
                Ok((location.id, location))
            })
            .collect::<CelebornResult<HashMap<_, _>>>()?;
        let mut worker_locations = HashMap::<WorkerIdentity, WorkerSlotLocations>::new();
        for location in response.all_primary_locations {
            let location = PartitionLocation::try_from(location)?;
            let worker_identity = location.worker_identity();
            worker_locations
                .entry(worker_identity)
                .or_insert_with(|| WorkerSlotLocations {
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
                CelebornPartitionLocation::from(request.old_location),
                request.cause,
            )
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))?;
        PartitionLocation::try_from(response.location.ok_or_else(|| {
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

    async fn report_metrics(&self, metrics: ApplicationMetrics) -> CelebornResult<()> {
        self.client
            .report_metrics(metrics.total_written, metrics.file_count)
            .await
            .map_err(|error| CelebornError::Application(error.to_string()))
    }

    async fn stop(&self) -> CelebornResult<()> {
        Ok(())
    }
}
