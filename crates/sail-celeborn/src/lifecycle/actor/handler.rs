use log::warn;
use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::common::{
    ApplicationMetrics, PartitionLocation, SlotReservation, UserIdentifier, WorkerSlotLocations,
};
use crate::error::{CelebornError, CelebornResult};
use crate::lifecycle::actor::{LifecycleManagerActor, ShuffleKey};
use crate::lifecycle::{LifecycleManagerMessage, ReviveRequest};
use crate::protocol::StatusCode;
use crate::worker::WorkerClientOptions;

impl LifecycleManagerActor {
    pub(super) fn handle_report_metrics(
        &mut self,
        metrics: ApplicationMetrics,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        self.application_metrics.add_assign(metrics);
        let _ = reply.send(Ok(()));
        ActorAction::Continue
    }

    pub(super) fn handle_heartbeat(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        ctx.send_with_delay(
            LifecycleManagerMessage::Heartbeat,
            self.options.heartbeat_interval,
        );
        if self.heartbeat_metrics.is_some() {
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let metrics = std::mem::take(&mut self.application_metrics);
        self.heartbeat_metrics = Some(metrics.clone());
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = client.heartbeat_application(application_id, metrics).await;
            let _ = handle
                .send(LifecycleManagerMessage::HeartbeatComplete { result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_heartbeat_complete(&mut self, result: CelebornResult<()>) -> ActorAction {
        if let Err(error) = result {
            if let Some(metrics) = self.heartbeat_metrics.take() {
                self.application_metrics.add_assign(metrics);
            }
            warn!("failed to send Celeborn application heartbeat: {error}");
        } else {
            self.heartbeat_metrics = None;
        }
        ActorAction::Continue
    }

    pub(super) fn handle_get_shuffle_id(
        &mut self,
        job_id: u64,
        stage: u64,
        reply: oneshot::Sender<CelebornResult<i32>>,
    ) -> ActorAction {
        let key = ShuffleKey { job_id, stage };
        let id = match self.shuffle_ids.get(&key) {
            Some(id) => Ok(*id),
            None => match self.next_shuffle_id.checked_add(1) {
                Some(next) => {
                    self.next_shuffle_id = next;
                    self.shuffle_ids.insert(key, next);
                    Ok(next)
                }
                None => Err(CelebornError::Application(
                    "shuffle ID space is exhausted".to_string(),
                )),
            },
        };
        let _ = reply.send(id);
        ActorAction::Continue
    }

    pub(super) fn handle_get_job_shuffle_ids(
        &mut self,
        job_id: u64,
        reply: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    ) -> ActorAction {
        let shuffle_ids = self
            .shuffle_ids
            .iter()
            .filter_map(|(key, &shuffle_id)| {
                (key.job_id == job_id).then_some((key.stage, shuffle_id))
            })
            .collect();
        let _ = reply.send(Ok(shuffle_ids));
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    ) -> ActorAction {
        if let Some(reservation) = self.reservations.get(&shuffle_id) {
            let _ = reply.send(Ok(reservation.clone()));
            return ActorAction::Continue;
        }
        if let Some(replies) = self.pending_slot_requests.get_mut(&shuffle_id) {
            replies.push(reply);
            return ActorAction::Continue;
        }
        self.pending_slot_requests.insert(shuffle_id, vec![reply]);
        if let Some(error) = self.application_registration.error() {
            if let Some(replies) = self.pending_slot_requests.remove(&shuffle_id) {
                for reply in replies {
                    let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                }
            }
            return ActorAction::Continue;
        }
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let hostname = self.options.hostname.clone();
        let user_identifier = self.user_identifier();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let partition_split_threshold = self.options.partition_split_threshold;
        let partition_split_mode = self.options.partition_split_mode;
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
                        Vec::new(),
                    )
                    .await?;
                for locations in reservation.worker_locations.values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .cloned()
                    else {
                        continue;
                    };
                    worker_clients
                        .client(
                            WorkerClientOptions::new(location)
                                .with_endpoint_resolver(endpoint_resolver.clone()),
                        )
                        .reserve_slots(
                            application_id.clone(),
                            shuffle_id,
                            locations.primary_locations.clone(),
                            locations.replica_locations.clone(),
                            user_identifier.clone(),
                            partition_split_threshold,
                            partition_split_mode,
                        )
                        .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::RegisterShuffleComplete { shuffle_id, result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_register_shuffle_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
    ) -> ActorAction {
        let replies = self
            .pending_slot_requests
            .remove(&shuffle_id)
            .unwrap_or_default();
        if let Ok(reservation) = &result {
            self.registered_shuffles
                .insert(shuffle_id, reservation.worker_locations.clone());
            self.reservations.insert(shuffle_id, reservation.clone());
            self.application_metrics.shuffle_count =
                self.application_metrics.shuffle_count.saturating_add(1);
            for reply in replies {
                let _ = reply.send(Ok(reservation.clone()));
            }
        } else if let Err(error) = result {
            for reply in replies {
                let _ = reply.send(Err(CelebornError::Application(error.to_string())));
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_revive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: ReviveRequest,
        reply: oneshot::Sender<CelebornResult<PartitionLocation>>,
    ) -> ActorAction {
        if let Some(error) = self.application_registration.error() {
            let _ = reply.send(Err(error));
            return ActorAction::Continue;
        }
        if self.committing_shuffles.contains(&request.shuffle_id)
            || self.committed_shuffles.contains(&request.shuffle_id)
        {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} has already ended",
                request.shuffle_id
            ))));
            return ActorAction::Continue;
        }
        let Some(current) = self
            .reservations
            .get(&request.shuffle_id)
            .and_then(|reservation| reservation.primary_locations.get(&request.partition_id))
            .cloned()
        else {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} partition {} is not registered",
                request.shuffle_id, request.partition_id
            ))));
            return ActorAction::Continue;
        };
        if current.epoch > request.old_location.epoch {
            let _ = reply.send(Ok(current));
            return ActorAction::Continue;
        }
        if current.epoch != request.old_location.epoch {
            let _ = reply.send(Err(CelebornError::Application(format!(
                "shuffle {} partition {} has an unexpected epoch",
                request.shuffle_id, request.partition_id
            ))));
            return ActorAction::Continue;
        }
        self.exclude_failed_workers(&request);
        let key = (request.shuffle_id, request.partition_id);
        if let Some(replies) = self.pending_revives.get_mut(&key) {
            replies.push(reply);
            return ActorAction::Continue;
        }
        self.pending_revives.insert(key, vec![reply]);

        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let hostname = self.options.hostname.clone();
        let user_identifier = self.user_identifier();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let worker_clients = self.worker_clients.clone();
        let partition_split_threshold = self.options.partition_split_threshold;
        let partition_split_mode = self.options.partition_split_mode;
        let excluded_workers = self.excluded_workers.values().cloned().collect();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let reservation = client
                    .request_slots(
                        application_id.clone(),
                        request.shuffle_id,
                        vec![request.partition_id],
                        hostname,
                        current.peer.is_some(),
                        1,
                        user_identifier.clone(),
                        excluded_workers,
                    )
                    .await?;
                let reservation =
                    reservation.with_epoch(current.epoch.checked_add(1).ok_or_else(|| {
                        CelebornError::Application("partition epoch is exhausted".to_string())
                    })?);
                for locations in reservation.worker_locations.values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .cloned()
                    else {
                        continue;
                    };
                    worker_clients
                        .client(
                            WorkerClientOptions::new(location)
                                .with_endpoint_resolver(endpoint_resolver.clone()),
                        )
                        .reserve_slots(
                            application_id.clone(),
                            request.shuffle_id,
                            locations.primary_locations.clone(),
                            locations.replica_locations.clone(),
                            user_identifier.clone(),
                            partition_split_threshold,
                            partition_split_mode,
                        )
                        .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::ReviveComplete {
                    shuffle_id: request.shuffle_id,
                    partition_id: request.partition_id,
                    result,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_revive_complete(
        &mut self,
        ctx: &mut ActorContext<Self>,
        shuffle_id: i32,
        partition_id: i32,
        result: CelebornResult<SlotReservation>,
    ) -> ActorAction {
        let replies = self
            .pending_revives
            .remove(&(shuffle_id, partition_id))
            .unwrap_or_default();
        match result {
            Ok(replacement) => {
                let Some(location) = replacement.primary_locations.get(&partition_id).cloned()
                else {
                    let error = CelebornError::Application(format!(
                        "revive for shuffle {shuffle_id} partition {partition_id} returned no location"
                    ));
                    for reply in replies {
                        let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                    }
                    return ActorAction::Continue;
                };
                let Some(reservation) = self.reservations.get_mut(&shuffle_id) else {
                    let client = self.client.clone();
                    let application_id = self.options.application_id.clone();
                    ctx.spawn(async move {
                        if let Err(error) = client.unregister_shuffle(application_id, shuffle_id).await {
                            warn!(
                                "failed to clean up Celeborn shuffle {shuffle_id} after a cancelled revive: {error}"
                            );
                        }
                    });
                    let error = CelebornError::Application(format!(
                        "revive for shuffle {shuffle_id} partition {partition_id} was cancelled"
                    ));
                    for reply in replies {
                        let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                    }
                    return ActorAction::Continue;
                };
                reservation
                    .primary_locations
                    .insert(partition_id, location.clone());
                merge_worker_locations(
                    &mut reservation.worker_locations,
                    replacement.worker_locations.clone(),
                );
                reservation.worker_ids.extend(replacement.worker_ids);
                reservation.worker_ids.sort();
                reservation.worker_ids.dedup();
                if let Some(registered) = self.registered_shuffles.get_mut(&shuffle_id) {
                    merge_worker_locations(registered, replacement.worker_locations);
                }
                for reply in replies {
                    let _ = reply.send(Ok(location.clone()));
                }
            }
            Err(error) => {
                for reply in replies {
                    let _ = reply.send(Err(CelebornError::Application(error.to_string())));
                }
            }
        }
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
        let worker_clients = self.worker_clients.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let mut metrics = ApplicationMetrics::default();
                for locations in worker_locations.into_values() {
                    let Some(location) = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                    else {
                        continue;
                    };
                    let commit_metrics = worker_clients
                        .client(
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
                    metrics.total_written = metrics
                        .total_written
                        .saturating_add(commit_metrics.total_written);
                    metrics.file_count =
                        metrics.file_count.saturating_add(commit_metrics.file_count);
                }
                Ok(metrics)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerMessage::MapperEndComplete {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_mapper_end_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<ApplicationMetrics>,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        self.committing_shuffles.remove(&shuffle_id);
        let result = match result {
            Ok(metrics) => {
                self.committed_shuffles.insert(shuffle_id);
                self.application_metrics.add_assign(metrics);
                Ok(())
            }
            Err(error) => Err(error),
        };
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle(
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
                .send(LifecycleManagerMessage::UnregisterShuffleComplete {
                    shuffle_id,
                    result,
                    reply,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_unregister_shuffle_complete(
        &mut self,
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    ) -> ActorAction {
        if result.is_ok() {
            self.shuffle_ids.retain(|_, id| *id != shuffle_id);
            self.registered_shuffles.remove(&shuffle_id);
            self.reservations.remove(&shuffle_id);
            self.mapper_attempts.remove(&shuffle_id);
            self.committing_shuffles.remove(&shuffle_id);
            self.committed_shuffles.remove(&shuffle_id);
        }
        let _ = reply.send(result);
        ActorAction::Continue
    }

    pub(super) fn handle_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
        result: oneshot::Sender<()>,
    ) -> ActorAction {
        let client = self.client.clone();
        let application_id = self.options.application_id.clone();
        let shuffle_ids = self.registered_shuffles.keys().copied().collect::<Vec<_>>();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            for shuffle_id in shuffle_ids {
                if let Err(error) = client
                    .unregister_shuffle(application_id.clone(), shuffle_id)
                    .await
                {
                    warn!(
                        "failed to unregister Celeborn shuffle {shuffle_id} while stopping: {error}"
                    );
                }
            }
            let _ = handle
                .send(LifecycleManagerMessage::StopComplete { result })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn user_identifier(&self) -> UserIdentifier {
        UserIdentifier {
            tenant_id: self.options.tenant_id.clone(),
            name: self.options.user_name.clone(),
        }
    }

    fn exclude_failed_workers(&mut self, request: &ReviveRequest) {
        let failed_locations = match request.cause {
            status
                if status == StatusCode::PushDataWriteFailPrimary as i32
                    || status == StatusCode::PushDataCreateConnectionFailPrimary as i32
                    || status == StatusCode::PushDataConnectionExceptionPrimary as i32
                    || status == StatusCode::PushDataTimeoutPrimary as i32
                    || status == StatusCode::PushDataPrimaryWorkerExcluded as i32 =>
            {
                vec![request.old_location.clone()]
            }
            status
                if status == StatusCode::PushDataWriteFailReplica as i32
                    || status == StatusCode::PushDataCreateConnectionFailReplica as i32
                    || status == StatusCode::PushDataConnectionExceptionReplica as i32
                    || status == StatusCode::PushDataTimeoutReplica as i32
                    || status == StatusCode::PushDataReplicaWorkerExcluded as i32 =>
            {
                request
                    .old_location
                    .peer
                    .iter()
                    .map(|peer| (**peer).clone())
                    .collect()
            }
            _ => Vec::new(),
        };
        for location in failed_locations {
            self.excluded_workers
                .entry(location.worker_identity())
                .or_insert(location);
        }
    }
}

impl SlotReservation {
    fn with_epoch(mut self, epoch: i32) -> Self {
        for location in self.primary_locations.values_mut() {
            location.set_epoch(epoch);
        }
        for locations in self.worker_locations.values_mut() {
            for location in &mut locations.primary_locations {
                location.set_epoch(epoch);
            }
            for location in &mut locations.replica_locations {
                location.set_epoch(epoch);
            }
        }
        self
    }
}

fn merge_worker_locations(
    target: &mut std::collections::HashMap<crate::common::WorkerIdentity, WorkerSlotLocations>,
    source: std::collections::HashMap<crate::common::WorkerIdentity, WorkerSlotLocations>,
) {
    for (worker_identity, locations) in source {
        let target_locations =
            target
                .entry(worker_identity)
                .or_insert_with(|| WorkerSlotLocations {
                    primary_locations: Vec::new(),
                    replica_locations: Vec::new(),
                });
        append_locations(
            &mut target_locations.primary_locations,
            locations.primary_locations,
        );
        append_locations(
            &mut target_locations.replica_locations,
            locations.replica_locations,
        );
    }
}

fn append_locations(target: &mut Vec<PartitionLocation>, source: Vec<PartitionLocation>) {
    for location in source {
        if !target
            .iter()
            .any(|existing| existing.unique_id() == location.unique_id())
        {
            target.push(location);
        }
    }
}
