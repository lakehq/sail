use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::{LifecycleManager, LifecycleManagerActor, LifecycleManagerMessage};
use crate::master::SlotReservation;

/// A lifecycle manager backed by a local actor.
#[derive(Debug, Clone)]
pub struct LocalLifecycleManager {
    handle: ActorHandle<LifecycleManagerActor>,
}

impl LocalLifecycleManager {
    pub fn new(handle: ActorHandle<LifecycleManagerActor>) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl LifecycleManager for LocalLifecycleManager {
    async fn request_slots(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::RequestSlotsBegin {
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::UnregisterShuffleBegin { shuffle_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::MapperEndBegin {
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn stop(&self) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::Stop { result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)
    }
}
