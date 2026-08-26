mod access;
mod actor;

pub use access::LocalLifecycleManager;
pub use actor::{LifecycleManagerActor, LifecycleManagerMessage, LifecycleManagerOptions};

use crate::common::{ApplicationMetrics, PartitionLocation, SlotReservation};
use crate::error::CelebornResult;

/// A failed push that needs a newer partition location.
#[derive(Debug, Clone)]
pub struct ReviveRequest {
    pub shuffle_id: i32,
    pub partition_id: i32,
    pub map_id: i32,
    pub attempt_id: i32,
    pub old_location: PartitionLocation,
    pub cause: i32,
}

/// Operations supported by a shuffle lifecycle manager.
#[tonic::async_trait]
pub trait LifecycleManager: Send + Sync + 'static {
    /// Get the stable shuffle ID associated with a shuffle.
    async fn get_shuffle_id(&self, job_id: u64, stage: u64) -> CelebornResult<i32>;

    /// Get all known shuffle IDs for a job, paired with their stage IDs.
    async fn get_job_shuffle_ids(&self, job_id: u64) -> CelebornResult<Vec<(u64, i32)>>;

    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation>;

    /// Allocate and reserve a higher-epoch location for a failed push.
    async fn revive(&self, request: ReviveRequest) -> CelebornResult<PartitionLocation>;

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> CelebornResult<()>;

    async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()>;

    /// Add metrics that will be reported in the next application heartbeat.
    async fn report_metrics(&self, metrics: ApplicationMetrics) -> CelebornResult<()>;

    async fn stop(&self) -> CelebornResult<()>;
}
