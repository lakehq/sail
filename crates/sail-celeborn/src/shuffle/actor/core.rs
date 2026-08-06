use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::shuffle::ShuffleClientEvent;
use crate::shuffle::actor::{ShuffleClientActor, ShuffleClientOptions};

#[tonic::async_trait]
impl Actor for ShuffleClientActor {
    type Message = ShuffleClientEvent;
    type Options = ShuffleClientOptions;

    fn name() -> &'static str {
        "CelebornShuffleClient"
    }

    fn new(options: Self::Options) -> Self {
        Self {
            application_id: options.application_id,
            lifecycle_manager: options.lifecycle_manager,
            locations: Default::default(),
            worker_locations: Default::default(),
            batch_ids: Default::default(),
            mapper_attempts: Default::default(),
            committing_shuffles: Default::default(),
            committed_shuffles: Default::default(),
            endpoint_resolver: options.endpoint_resolver,
        }
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            ShuffleClientEvent::RegisterShuffle {
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
            ShuffleClientEvent::RegisterShuffleEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_register_shuffle_end(shuffle_id, result, reply),
            ShuffleClientEvent::PushData {
                shuffle_id,
                partition_id,
                map_id,
                attempt_id,
                data,
                result,
            } => self.handle_push_data(
                ctx,
                shuffle_id,
                partition_id,
                map_id,
                attempt_id,
                data,
                result,
            ),
            ShuffleClientEvent::MapperEnd {
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            } => self.handle_mapper_end(ctx, shuffle_id, map_id, attempt_id, num_mappers, result),
            ShuffleClientEvent::MapperEndCommitEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_mapper_end_commit_end(shuffle_id, result, reply),
            ShuffleClientEvent::ReadPartition {
                shuffle_id,
                partition_id,
                result,
            } => self.handle_read_partition(ctx, shuffle_id, partition_id, result),
            ShuffleClientEvent::UnregisterShuffle { shuffle_id, result } => {
                self.handle_unregister_shuffle(ctx, shuffle_id, result)
            }
            ShuffleClientEvent::UnregisterShuffleEnd {
                shuffle_id,
                result,
                reply,
            } => self.handle_unregister_shuffle_end(shuffle_id, result, reply),
            ShuffleClientEvent::Stop { result } => self.handle_stop(result),
        }
    }
}
