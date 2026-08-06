use std::sync::Arc;

use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::error::{CelebornError, CelebornResult};
use crate::master::SlotReservation;
use crate::shuffle::ShuffleClientEvent;
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
        let lifecycle_manager = Arc::clone(&self.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager
                .request_slots(shuffle_id, partition_ids, should_replicate, max_workers)
                .await;
            let _ = handle
                .send(ShuffleClientEvent::RegisterShuffleEnd {
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
            self.worker_locations
                .insert(shuffle_id, reservation.worker_locations.clone());
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
            let _ = reply.send(Err(unregistered_partition(shuffle_id, partition_id)));
            return ActorAction::Continue;
        };
        let batch_id = self
            .batch_ids
            .entry((shuffle_id, map_id, attempt_id))
            .or_default();
        let current_batch_id = *batch_id;
        *batch_id += 1;
        let shuffle_key = self.shuffle_key(shuffle_id);
        let endpoint_resolver = self.endpoint_resolver.clone();
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
        let num_mappers = match usize::try_from(num_mappers) {
            Ok(num_mappers) if num_mappers > 0 => num_mappers,
            _ => {
                let _ = reply.send(Err(CelebornError::Application(
                    "number of mappers must be positive".to_string(),
                )));
                return ActorAction::Continue;
            }
        };
        let map_id = match usize::try_from(map_id) {
            Ok(map_id) if map_id < num_mappers => map_id,
            _ => {
                let _ = reply.send(Err(CelebornError::Application(format!(
                    "map ID {map_id} is outside the mapper range"
                ))));
                return ActorAction::Continue;
            }
        };
        if self.committed_shuffles.contains(&shuffle_id) {
            let _ = reply.send(Ok(()));
            return ActorAction::Continue;
        }
        let map_attempts = {
            let attempts = self
                .mapper_attempts
                .entry(shuffle_id)
                .or_insert_with(|| vec![-1; num_mappers]);
            if attempts.len() != num_mappers {
                let _ = reply.send(Err(CelebornError::Application(format!(
                    "shuffle {shuffle_id} was registered with {} mappers, not {num_mappers}",
                    attempts.len()
                ))));
                return ActorAction::Continue;
            }
            // Match Celeborn's speculative execution semantics: the first completed attempt
            // wins and later attempts are acknowledged without changing the committed output.
            if attempts[map_id] != -1 {
                let _ = reply.send(Ok(()));
                return ActorAction::Continue;
            }
            attempts[map_id] = attempt_id;
            if attempts.contains(&-1) {
                let _ = reply.send(Ok(()));
                return ActorAction::Continue;
            }
            attempts.clone()
        };
        self.committing_shuffles.insert(shuffle_id);
        let application_id = self.application_id.clone();
        let worker_locations = self
            .worker_locations
            .get(&shuffle_id)
            .cloned()
            .unwrap_or_default();
        let endpoint_resolver = self.endpoint_resolver.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                for locations in worker_locations.into_values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                    else {
                        continue;
                    };
                    WorkerClient::new(
                        WorkerClientOptions::new(location.clone())
                            .with_endpoint_resolver(endpoint_resolver.clone()),
                    )
                    .commit_files(
                        application_id.clone(),
                        shuffle_id,
                        locations.primary_locations,
                        locations.replica_locations,
                        map_attempts.clone(),
                    )
                    .await?;
                }
                Ok(())
            }
            .await;
            let _ = handle
                .send(ShuffleClientEvent::MapperEndCommitEnd {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_mapper_end_commit_end(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        self.committing_shuffles.remove(&shuffle_id);
        if result.is_ok() {
            self.committed_shuffles.insert(shuffle_id);
        }
        let _ = reply.send(result);
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
            let _ = reply.send(Err(unregistered_partition(shuffle_id, partition_id)));
            return ActorAction::Continue;
        };
        let shuffle_key = self.shuffle_key(shuffle_id);
        let endpoint_resolver = self.endpoint_resolver.clone();
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

    pub(super) fn handle_unregister_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        let lifecycle_manager = Arc::clone(&self.lifecycle_manager);
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = lifecycle_manager.unregister_shuffle(shuffle_id).await;
            let _ = handle
                .send(ShuffleClientEvent::UnregisterShuffleEnd {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle_end(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        if result.is_ok() {
            self.locations
                .retain(|(registered_shuffle_id, _), _| *registered_shuffle_id != shuffle_id);
            self.worker_locations.remove(&shuffle_id);
            self.batch_ids
                .retain(|(registered_shuffle_id, _, _), _| *registered_shuffle_id != shuffle_id);
            self.mapper_attempts.remove(&shuffle_id);
            self.committing_shuffles.remove(&shuffle_id);
            self.committed_shuffles.remove(&shuffle_id);
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_stop(&mut self, reply: oneshot::Sender<()>) -> ActorAction {
        let _ = reply.send(());
        ActorAction::Stop
    }

    fn shuffle_key(&self, shuffle_id: i32) -> String {
        format!("{}-{shuffle_id}", self.application_id)
    }
}

fn unregistered_partition(shuffle_id: i32, partition_id: i32) -> CelebornError {
    CelebornError::Application(format!(
        "shuffle {shuffle_id} partition {partition_id} is not registered"
    ))
}
