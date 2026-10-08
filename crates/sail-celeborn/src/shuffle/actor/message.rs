use std::borrow::Cow;

use bytes::Bytes;
use futures::stream::BoxStream;
use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::common::{PartitionLocation, SlotReservation};
use crate::error::CelebornResult;

pub enum ShuffleClientMessage {
    GetShuffleId {
        job_id: u64,
        stage: u64,
        result: oneshot::Sender<CelebornResult<i32>>,
    },
    GetShuffleIdComplete {
        job_id: u64,
        stage: u64,
        result: CelebornResult<i32>,
        reply: oneshot::Sender<CelebornResult<i32>>,
    },
    GetJobShuffleIds {
        job_id: u64,
        result: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    },
    GetJobShuffleIdsComplete {
        job_id: u64,
        result: CelebornResult<Vec<(u64, i32)>>,
        reply: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
    },
    RegisterShuffle {
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        result: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    RegisterShuffleComplete {
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    PushData {
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Bytes,
        result: oneshot::Sender<CelebornResult<usize>>,
    },
    PushDataComplete {
        shuffle_id: i32,
        partition_id: i32,
        location: Option<PartitionLocation>,
        result: CelebornResult<usize>,
        reply: oneshot::Sender<CelebornResult<usize>>,
    },
    MapperEnd {
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    UnregisterShuffle {
        shuffle_id: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    UnregisterShuffleComplete {
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    },
    /// Remove local client state for a shuffle without unregistering it.
    CleanUpShuffle {
        shuffle_id: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    ReadPartitionStream {
        shuffle_id: i32,
        partition_id: i32,
        result: oneshot::Sender<BoxStream<'static, CelebornResult<Bytes>>>,
    },
    ReadPartitionStreamComplete {
        shuffle_id: i32,
        partition_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<BoxStream<'static, CelebornResult<Bytes>>>,
    },
    Stop {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for ShuffleClientMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::GetShuffleId { .. } => "GetShuffleId",
            Self::GetShuffleIdComplete { .. } => "GetShuffleIdComplete",
            Self::GetJobShuffleIds { .. } => "GetJobShuffleIds",
            Self::GetJobShuffleIdsComplete { .. } => "GetJobShuffleIdsComplete",
            Self::RegisterShuffle { .. } => "RegisterShuffle",
            Self::RegisterShuffleComplete { .. } => "RegisterShuffleComplete",
            Self::PushData { .. } => "PushData",
            Self::PushDataComplete { .. } => "PushDataComplete",
            Self::MapperEnd { .. } => "MapperEnd",
            Self::UnregisterShuffle { .. } => "UnregisterShuffle",
            Self::UnregisterShuffleComplete { .. } => "UnregisterShuffleComplete",
            Self::CleanUpShuffle { .. } => "CleanUpShuffle",
            Self::ReadPartitionStream { .. } => "ReadPartitionStream",
            Self::ReadPartitionStreamComplete { .. } => "ReadPartitionStreamComplete",
            Self::Stop { .. } => "Stop",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
