use bytes::Bytes;
use futures::stream::{self, BoxStream};
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::common::SlotReservation;
use crate::error::{CelebornError, CelebornResult};
use crate::shuffle::{ShuffleClientActor, ShuffleClientMessage};

/// A local handle to a Celeborn shuffle client actor.
#[derive(Debug, Clone)]
pub struct ShuffleClient {
    handle: ActorHandle<ShuffleClientActor>,
}

impl ShuffleClient {
    pub async fn get_shuffle_id(&self, job_id: u64, stage: u64) -> CelebornResult<i32> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::GetShuffleId {
                job_id,
                stage,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    pub async fn get_job_shuffle_ids(&self, job_id: u64) -> CelebornResult<Vec<(u64, i32)>> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::GetJobShuffleIds { job_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    pub fn new(handle: ActorHandle<ShuffleClientActor>) -> Self {
        Self { handle }
    }

    pub async fn push_data(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Bytes,
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

    pub async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::UnregisterShuffle { shuffle_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    pub async fn clean_up_shuffle(&self, shuffle_id: i32) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(ShuffleClientMessage::CleanUpShuffle { shuffle_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    pub async fn read_partition_stream(
        &self,
        shuffle_id: i32,
        partition_id: i32,
    ) -> BoxStream<'static, CelebornResult<Bytes>> {
        let (result, receiver) = oneshot::channel();
        if self
            .handle
            .send(ShuffleClientMessage::ReadPartitionStream {
                shuffle_id,
                partition_id,
                result,
            })
            .await
            .is_err()
        {
            return Box::pin(stream::once(async { Err(CelebornError::ActorStopped) }));
        }
        receiver
            .await
            .unwrap_or_else(|_| Box::pin(stream::once(async { Err(CelebornError::ActorStopped) })))
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
