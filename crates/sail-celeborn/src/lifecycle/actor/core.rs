use log::warn;
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::lifecycle::actor::{ApplicationRegistration, LifecycleManagerActor};
use crate::lifecycle::event::LifecycleManagerEvent;
use crate::lifecycle::options::LifecycleManagerOptions;
use crate::master::MasterClient;

#[tonic::async_trait]
impl Actor for LifecycleManagerActor {
    type Message = LifecycleManagerEvent;
    type Options = LifecycleManagerOptions;

    fn name() -> &'static str {
        "CelebornLifecycleManager"
    }

    fn new(options: Self::Options) -> Self {
        Self {
            client: MasterClient::new(options.master.clone()),
            options,
            registered_shuffles: Default::default(),
            application_registration: ApplicationRegistration::Pending,
        }
    }

    async fn start(&mut self, _ctx: &mut ActorContext<Self>) {
        self.application_registration = match self
            .client
            .register_application(self.options.application_id.clone(), self.user_identifier())
            .await
        {
            Ok(()) => ApplicationRegistration::Succeeded,
            Err(error) => {
                let reason = error.to_string();
                warn!("failed to register Celeborn application: {reason}");
                ApplicationRegistration::Failed { reason }
            }
        };
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            LifecycleManagerEvent::RequestSlotsBegin {
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            } => self.handle_request_slots(
                ctx,
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            ),
            LifecycleManagerEvent::RequestSlotsEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_request_slots_complete(shuffle_id, result, reply),
            LifecycleManagerEvent::UnregisterShuffleBegin { shuffle_id, result } => {
                self.handle_unregister_shuffle(ctx, shuffle_id, result)
            }
            LifecycleManagerEvent::UnregisterShuffleEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_unregister_shuffle_complete(shuffle_id, result, reply),
            LifecycleManagerEvent::Stop { result } => {
                // TODO: unregister shuffles before stopping to release slots early
                let _ = result.send(());
                ActorAction::Stop
            }
        }
    }
}
