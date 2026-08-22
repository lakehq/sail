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
            worker_clients: Default::default(),
            excluded_workers: Default::default(),
            registered_shuffles: Default::default(),
            reservations: Default::default(),
            pending_slot_requests: Default::default(),
            pending_revives: Default::default(),
            mapper_attempts: Default::default(),
            committing_shuffles: Default::default(),
            committed_shuffles: Default::default(),
            shuffle_ids: Default::default(),
            next_shuffle_id: 0,
            application_registration: ApplicationRegistration::Pending,
            application_metrics: Default::default(),
            heartbeat_metrics: None,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
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
        if matches!(
            self.application_registration,
            ApplicationRegistration::Succeeded
        ) {
            self.application_metrics.application_count = 1;
            ctx.send(LifecycleManagerMessage::Heartbeat);
        }
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            LifecycleManagerMessage::GetShuffleId {
                job_id,
                stage,
                result,
            } => self.handle_get_shuffle_id(job_id, stage, result),
            LifecycleManagerMessage::GetJobShuffleIds { job_id, result } => {
                self.handle_get_job_shuffle_ids(job_id, result)
            }
            LifecycleManagerMessage::RegisterShuffle {
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            } => self.handle_register_shuffle(
                ctx,
                shuffle_id,
                partition_ids,
                should_replicate,
                max_workers,
                result,
            ),
            LifecycleManagerMessage::RegisterShuffleComplete { shuffle_id, result } => {
                self.handle_register_shuffle_complete(shuffle_id, result)
            }
            LifecycleManagerMessage::Revive { request, result } => {
                self.handle_revive(ctx, request, result)
            }
            LifecycleManagerMessage::ReviveComplete {
                shuffle_id,
                partition_id,
                result,
            } => self.handle_revive_complete(ctx, shuffle_id, partition_id, result),
            LifecycleManagerMessage::MapperEnd {
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            } => self.handle_mapper_end(ctx, shuffle_id, map_id, attempt_id, num_mappers, result),
            LifecycleManagerMessage::MapperEndComplete {
                shuffle_id,
                result,
                reply,
            } => self.handle_mapper_end_complete(shuffle_id, result, reply),
            LifecycleManagerMessage::UnregisterShuffle { shuffle_id, result } => {
                self.handle_unregister_shuffle(ctx, shuffle_id, result)
            }
            LifecycleManagerMessage::UnregisterShuffleComplete {
                shuffle_id,
                result,
                reply,
            } => self.handle_unregister_shuffle_complete(shuffle_id, result, reply),
            LifecycleManagerMessage::ReportMetrics { metrics, result } => {
                self.handle_report_metrics(metrics, result)
            }
            LifecycleManagerMessage::Heartbeat => self.handle_heartbeat(ctx),
            LifecycleManagerMessage::HeartbeatComplete { result } => {
                self.handle_heartbeat_complete(result)
            }
            LifecycleManagerMessage::Stop { result } => self.handle_stop(ctx, result),
            LifecycleManagerMessage::StopComplete { result } => {
                let _ = result.send(());
                ActorAction::Stop
            }
        }
    }
}
