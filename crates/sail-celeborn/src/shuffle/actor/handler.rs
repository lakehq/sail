use std::sync::Arc;

use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::error::{CelebornError, CelebornResult};
use crate::master::SlotReservation;
use crate::shuffle::ShuffleClientMessage;
use crate::shuffle::actor::ShuffleClientActor;
use crate::worker::{WorkerClient, WorkerClientOptions};

impl ShuffleClientActor {
    pub(super) fn handle_register_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager
                .request_slots(shuffle_id, partition_ids, should_replicate, max_workers)
                .await;
            let _ = handle
                .send(ShuffleClientMessage::RegisterShuffleEnd {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle_end(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        if let Ok(reservation) = &result {
            self.locations.extend(
                reservation
                    .primary_locations
                    .iter()
                    .map(|(&partition_id, location)| {
                        ((shuffle_id, partition_id), location.clone())
                    }),
            );
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_push_data(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Vec<u8>,
        reply: oneshot::Sender<CelebornResult<usize>>,
    ) -> ActorAction {
        let Some(location) = self.locations.get(&(shuffle_id, partition_id)).cloned() else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {shuffle_id} partition {partition_id} is not registered"
            ))));
            return ActorAction::Continue;
        };
        let batch_id = self
            .batch_ids
            .entry((shuffle_id, map_id, attempt_id))
            .or_default();
        let current_batch_id = *batch_id;
        *batch_id += 1;
        let shuffle_key = self.shuffle_key(shuffle_id);
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        ctx.spawn(async move {
            let result = WorkerClient::new(
                WorkerClientOptions::new(location).with_endpoint_resolver(endpoint_resolver),
            )
            .push_data(&shuffle_key, map_id, attempt_id, current_batch_id, &data)
            .await;
            let _ = reply.send(result);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_mapper_end(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.options.lifecycle_manager);
        ctx.spawn(async move {
            let _ = reply.send(
                lifecycle_manager
                    .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                    .await,
            );
        });
        ActorAction::Continue
    }

    pub(super) fn handle_read_partition(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        reply: oneshot::Sender<CelebornResult<Vec<u8>>>,
    ) -> ActorAction {
        let Some(location) = self.locations.get(&(shuffle_id, partition_id)).cloned() else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {shuffle_id} partition {partition_id} is not registered"
            ))));
            return ActorAction::Continue;
        };
        let shuffle_key = self.shuffle_key(shuffle_id);
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        ctx.spawn(async move {
            let result = WorkerClient::new(
                WorkerClientOptions::new(location).with_endpoint_resolver(endpoint_resolver),
            )
            .read_partition(&shuffle_key)
            .await;
            let _ = reply.send(result);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_stop(&mut self, reply: oneshot::Sender<()>) -> ActorAction {
        let _ = reply.send(());
        ActorAction::Stop
    }

    fn shuffle_key(&self, shuffle_id: i32) -> String {
        format!("{}-{shuffle_id}", self.options.application_id)
    }
}
