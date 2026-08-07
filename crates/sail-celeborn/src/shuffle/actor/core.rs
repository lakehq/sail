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
            options,
            locations: Default::default(),
            batch_ids: Default::default(),
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
            ShuffleClientEvent::ReadPartition {
                shuffle_id,
                partition_id,
                result,
            } => self.handle_read_partition(ctx, shuffle_id, partition_id, result),
            ShuffleClientEvent::Stop { result } => self.handle_stop(result),
        }
    }
}
