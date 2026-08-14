use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::shuffle::ShuffleClientMessage;
use crate::shuffle::actor::{ShuffleClientActor, ShuffleClientOptions};

#[tonic::async_trait]
impl Actor for ShuffleClientActor {
    type Message = ShuffleClientMessage;
    type Options = ShuffleClientOptions;

    fn name() -> &'static str {
        "CelebornShuffleClient"
    }

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            shuffle_ids: Default::default(),
            locations: Default::default(),
            location_history: Default::default(),
            worker_clients: Default::default(),
            batch_ids: Default::default(),
        }
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            ShuffleClientMessage::GetShuffleId {
                job_id,
                stage,
                result,
            } => self.handle_get_shuffle_id(ctx, job_id, stage, result),
            ShuffleClientMessage::GetShuffleIdComplete {
                job_id,
                stage,
                result,
                reply,
            } => self.handle_get_shuffle_id_complete(job_id, stage, result, reply),
            ShuffleClientMessage::GetJobShuffleIds { job_id, result } => {
                self.handle_get_job_shuffle_ids(ctx, job_id, result)
            }
            ShuffleClientMessage::GetJobShuffleIdsComplete {
                job_id,
                result,
                reply,
            } => self.handle_get_job_shuffle_ids_complete(job_id, result, reply),
            ShuffleClientMessage::RegisterShuffle {
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
            ShuffleClientMessage::RegisterShuffleComplete {
                shuffle_id,
                result,
                reply,
            } => self.handle_register_shuffle_complete(shuffle_id, result, reply),
            ShuffleClientMessage::PushData {
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
            ShuffleClientMessage::PushDataComplete {
                shuffle_id,
                partition_id,
                location,
                result,
                reply,
            } => self.handle_push_data_complete(shuffle_id, partition_id, location, result, reply),
            ShuffleClientMessage::MapperEnd {
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                result,
            } => self.handle_mapper_end(ctx, shuffle_id, map_id, attempt_id, num_mappers, result),
            ShuffleClientMessage::UnregisterShuffle { shuffle_id, result } => {
                self.handle_unregister_shuffle(ctx, shuffle_id, result)
            }
            ShuffleClientMessage::UnregisterShuffleComplete {
                shuffle_id,
                result,
                reply,
            } => self.handle_unregister_shuffle_complete(shuffle_id, result, reply),
            ShuffleClientMessage::CleanUpShuffle { shuffle_id, result } => {
                self.handle_clean_up_shuffle(shuffle_id, result)
            }
            ShuffleClientMessage::ReadPartitionStream {
                shuffle_id,
                partition_id,
                result,
            } => self.handle_read_partition_stream(ctx, shuffle_id, partition_id, result),
            ShuffleClientMessage::ReadPartitionStreamComplete {
                shuffle_id,
                partition_id,
                result,
                reply,
            } => self.handle_read_partition_stream_complete(
                ctx,
                shuffle_id,
                partition_id,
                result,
                reply,
            ),
            ShuffleClientMessage::Stop { result } => self.handle_stop(result),
        }
    }
}
