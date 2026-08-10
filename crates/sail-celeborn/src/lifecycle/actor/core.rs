use log::warn;
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::lifecycle::actor::{ApplicationRegistration, LifecycleManagerActor};
use crate::lifecycle::{LifecycleManagerMessage, LifecycleManagerOptions};
use crate::master::MasterClient;

#[tonic::async_trait]
impl Actor for LifecycleManagerActor {
    type Message = LifecycleManagerMessage;
    type Options = LifecycleManagerOptions;

    fn name() -> &'static str {
        "CelebornLifecycleManager"
    }

    fn new(options: Self::Options) -> Self {
        let client = MasterClient::new(options.master.clone());
        Self {
            options,
            client,
            registered_shuffles: Default::default(),
            reservations: Default::default(),
            pending_slot_requests: Default::default(),
            mapper_attempts: Default::default(),
            committing_shuffles: Default::default(),
            committed_shuffles: Default::default(),
            shuffle_ids: Default::default(),
            next_shuffle_id: 0,
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
            LifecycleManagerMessage::CreateShuffleId {
                shuffle_key,
                result,
            } => self.handle_create_shuffle_id(shuffle_key, result),
            LifecycleManagerMessage::RequestSlotsBegin {
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            } => self.handle_request_slots_begin(
                ctx,
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            ),
            LifecycleManagerMessage::RequestSlotsEnd { shuffle_id, result } => {
                self.handle_request_slots_end(shuffle_id, result)
            }
            LifecycleManagerMessage::MapperEndBegin {
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            } => self.handle_mapper_end_begin(
                ctx,
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            ),
            LifecycleManagerMessage::MapperEndCommitEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_mapper_end_commit_end(shuffle_id, result, reply),
            LifecycleManagerMessage::UnregisterShuffleBegin { shuffle_id, result } => {
                self.handle_unregister_shuffle_begin(ctx, shuffle_id, result)
            }
            LifecycleManagerMessage::UnregisterShuffleEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_unregister_shuffle_end(shuffle_id, result, reply),
            LifecycleManagerMessage::Stop { result } => self.handle_stop(ctx, result),
            LifecycleManagerMessage::StopEnd { result } => {
                let _ = result.send(());
                ActorAction::Stop
            }
        }
    }
}
