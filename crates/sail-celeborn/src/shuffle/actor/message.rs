use std::borrow::Cow;

use futures::stream::BoxStream;
use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::error::CelebornResult;
use crate::master::SlotReservation;

pub enum ShuffleClientMessage {
    CreateShuffleId {
        shuffle_key: String,
        result: oneshot::Sender<CelebornResult<i32>>,
    },
    RegisterShuffle {
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        result: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    RegisterShuffleEnd {
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    PushData {
        shuffle_id: i32,
        partition_id: i32,
        map_id: i32,
        attempt_id: i32,
        data: Vec<u8>,
        result: oneshot::Sender<CelebornResult<usize>>,
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
    ClearShuffle {
        shuffle_id: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    ReadPartitionStream {
        shuffle_id: i32,
        partition_id: i32,
        result: oneshot::Sender<CelebornResult<BoxStream<'static, CelebornResult<Vec<u8>>>>>,
    },
    Stop {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for ShuffleClientMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::CreateShuffleId { .. } => "CreateShuffleId",
            Self::RegisterShuffle { .. } => "RegisterShuffle",
            Self::RegisterShuffleEnd { .. } => "RegisterShuffleEnd",
            Self::PushData { .. } => "PushData",
            Self::MapperEnd { .. } => "MapperEnd",
            Self::UnregisterShuffle { .. } => "UnregisterShuffle",
            Self::ClearShuffle { .. } => "ClearShuffle",
            Self::ReadPartitionStream { .. } => "ReadPartitionStream",
            Self::Stop { .. } => "Stop",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
