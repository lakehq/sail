use std::borrow::Cow;

use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::error::CelebornResult;
use crate::master::SlotReservation;

pub enum ShuffleClientMessage {
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
    ReadPartition {
        shuffle_id: i32,
        partition_id: i32,
        result: oneshot::Sender<CelebornResult<Vec<u8>>>,
    },
    Stop {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for ShuffleClientMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::RegisterShuffle { .. } => "RegisterShuffle",
            Self::RegisterShuffleEnd { .. } => "RegisterShuffleEnd",
            Self::PushData { .. } => "PushData",
            Self::MapperEnd { .. } => "MapperEnd",
            Self::ReadPartition { .. } => "ReadPartition",
            Self::Stop { .. } => "Stop",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
