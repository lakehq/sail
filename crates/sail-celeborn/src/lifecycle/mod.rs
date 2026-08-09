mod access;
mod actor;

pub use access::LocalLifecycleManager;
pub use actor::{LifecycleManagerActor, LifecycleManagerMessage, LifecycleManagerOptions};

use crate::error::CelebornResult;
use crate::master::SlotReservation;

/// Operations supported by a Celeborn shuffle lifecycle manager.
#[tonic::async_trait]
pub trait LifecycleManager: Send + Sync + 'static {
    async fn request_slots(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation>;

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> CelebornResult<()>;

    async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()>;

    async fn stop(&self) -> CelebornResult<()>;
}
