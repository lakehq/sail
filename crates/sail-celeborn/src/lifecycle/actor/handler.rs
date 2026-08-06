use sail_common::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::error::CelebornResult;
use crate::lifecycle::actor::LifecycleManagerActor;
use crate::lifecycle::event::LifecycleManagerEvent;
use crate::master::{SlotReservation, UserIdentifier};

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
        let reserve_application_id = application_id.clone();
        let reserve_user_identifier = user_identifier.clone();
        let endpoint_resolver = self.options.endpoint_resolver.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = async {
                let reservation = client
                    .request_slots(
                        application_id,
                        shuffle_id,
                        partition_ids,
                        hostname,
                        should_replicate,
                        max_workers,
                        user_identifier,
                    )
                    .await?;
                for locations in reservation.worker_locations.values() {
                    let location = locations
                        .primary_locations
                        .first()
                        .or_else(|| locations.replica_locations.first())
                        .ok_or_else(|| {
                            crate::error::CelebornError::Protocol(
                                "worker reservation has no partition locations".to_string(),
                            )
                        })?
                        .clone();
                    crate::worker::WorkerClient::new(
                        crate::worker::WorkerClientOptions::new(location)
                            .with_endpoint_resolver(endpoint_resolver.clone()),
                    )
                    .reserve_slots(
                        reserve_application_id.clone(),
                        shuffle_id,
                        locations.primary_locations.clone(),
                        locations.replica_locations.clone(),
                        reserve_user_identifier.clone(),
                    )
                    .await?;
                }
                Ok(reservation)
            }
            .await;
            let _ = handle
                .send(LifecycleManagerEvent::RequestSlotsEnd {
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
        if result.is_ok() {
            self.registered_shuffles.insert(shuffle_id);
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
                .send(LifecycleManagerEvent::UnregisterShuffleEnd {
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
