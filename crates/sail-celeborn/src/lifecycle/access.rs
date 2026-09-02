use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::common::{ApplicationMetrics, PartitionLocation, SlotReservation};
use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::{
    LifecycleManager, LifecycleManagerActor, LifecycleManagerMessage, ReviveRequest,
};

/// A lifecycle manager backed by a local actor.
#[derive(Debug, Clone)]
pub struct LocalLifecycleManager {
    handle: ActorHandle<LifecycleManagerActor>,
}

impl LocalLifecycleManager {
    pub fn new(handle: ActorHandle<LifecycleManagerActor>) -> Self {
        Self { handle }
    }

    pub fn handle(&self) -> ActorHandle<LifecycleManagerActor> {
        self.handle.clone()
    }
}

#[tonic::async_trait]
impl LifecycleManager for LocalLifecycleManager {
    async fn get_shuffle_id(&self, job_id: u64, stage: u64) -> CelebornResult<i32> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::GetShuffleId {
                job_id,
                stage,
                result,
            })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn get_job_shuffle_ids(&self, job_id: u64) -> CelebornResult<Vec<(u64, i32)>> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::GetJobShuffleIds { job_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
    ) -> CelebornResult<SlotReservation> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::RegisterShuffle {
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

    async fn revive(&self, request: ReviveRequest) -> CelebornResult<PartitionLocation> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::Revive { request, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn unregister_shuffle(&self, shuffle_id: i32) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::UnregisterShuffle { shuffle_id, result })
            .await
            .map_err(|_| CelebornError::ActorStopped)?;
        receiver.await.map_err(|_| CelebornError::ActorStopped)?
    }

    async fn report_metrics(&self, metrics: ApplicationMetrics) -> CelebornResult<()> {
        let (result, receiver) = oneshot::channel();
        self.handle
            .send(LifecycleManagerMessage::ReportMetrics { metrics, result })
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
            .send(LifecycleManagerMessage::MapperEnd {
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
