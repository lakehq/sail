use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::{CelebornError, CelebornResult};
use crate::master::SlotReservation;
use crate::shuffle::{ShuffleClientActor, ShuffleClientMessage};

/// A local handle to a Celeborn shuffle client actor.
#[derive(Debug, Clone)]
pub struct ShuffleClient {
    handle: ActorHandle<ShuffleClientActor>,
}

impl ShuffleClient {
    pub fn new(handle: ActorHandle<ShuffleClientActor>) -> Self {
        Self { handle }
    }

    pub async fn push_data(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Vec<u8>,
    ) -> CelebornResult<usize> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::PushData {
                shuffle_id,
                partition_id,
                map_id,
                attempt_id,
                data,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::MapperEnd {
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

    pub async fn read_partition(
        &self,
        shuffle_id: i32,
        partition_id: i32,
    ) -> CelebornResult<Vec<u8>> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::ReadPartition {
                shuffle_id,
                partition_id,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    /// Register a shuffle and reserve slots for its reduce partitions.
    pub async fn register_shuffle(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::RegisterShuffle {
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

    /// Stop the shuffle client.
    pub async fn stop(&self) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::Stop { result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)
    }
}
