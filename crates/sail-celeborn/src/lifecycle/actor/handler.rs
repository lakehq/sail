use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::LifecycleManagerMessage;
use crate::lifecycle::actor::LifecycleManagerActor;
use crate::master::{SlotReservation, UserIdentifier};
use crate::worker::{WorkerClient, WorkerClientOptions};

impl LifecycleManagerActor {
    pub(super) fn handle_request_slots_begin(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        if let Some(error) = self.application_registration.error() {
            let _ = reply.send(Err(error));
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let hostname = self.options.hostname.clone();
        let user_identifier = self.user_identifier();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let reservation = client
                    .request_slots(
                        application_id.clone(),
                        shuffle_id,
                        partition_ids,
                        hostname,
                        should_replicate,
                        max_workers,
                        user_identifier.clone(),
                    )
                    .await?;
                for locations in reservation.worker_locations.values() {
                    let location = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .ok_or_else(|| {
                            CelebornError::Protocol(
                                "worker reservation has no partition locations".to_string(),
                            )
                        })?
                        .clone();
                    WorkerClient::new(
                        WorkerClientOptions::new(location)
                            .with_endpoint_resolver(endpoint_resolver.clone()),
                    )
                    .reserve_slots(
                        application_id.clone(),
                        shuffle_id,
                        locations.primary_locations.clone(),
                        locations.replica_locations.clone(),
                        user_identifier.clone(),
                    )
                    .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::RequestSlotsEnd {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_request_slots_end(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        if let Ok(reservation) = &result {
            self.registered_shuffles
                .insert(shuffle_id, reservation.worker_locations.clone());
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_mapper_end_begin(
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
        let Some(worker_locations) = self.registered_shuffles.get(&shuffle_id).cloned() else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {shuffle_id} is not registered"
            ))));
            return ActorAction::Continue;
        };
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
        let application_id = self.options.application_id.clone();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
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
                .send(LifecycleManagerMessage::MapperEndCommitEnd {
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

    pub(super) fn handle_unregister_shuffle_begin(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        if let Some(error) = self.application_registration.error() {
            let _ = reply.send(Err(error));
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = client.unregister_shuffle(application_id, shuffle_id).await;
            let _ = handle
                .send(LifecycleManagerMessage::UnregisterShuffleEnd {
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
            self.registered_shuffles.remove(&shuffle_id);
            self.mapper_attempts.remove(&shuffle_id);
            self.committing_shuffles.remove(&shuffle_id);
            self.committed_shuffles.remove(&shuffle_id);
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn user_identifier(&self) -> UserIdentifier {
        UserIdentifier {
            tenant_id: self.options.tenant_id.clone(),
            name: self.options.user_name.clone(),
        }
    }
}
