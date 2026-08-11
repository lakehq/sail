use std::borrow::Cow;

use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::error::CelebornResult;
use crate::master::SlotReservation;

pub enum LifecycleManagerMessage {
    GetOrCreateShuffleId {
        job_id: u64,
        stage: u64,
        result: oneshot::Sender<CelebornResult<i32>>,
    },
    GetShuffleIds {
        job_id: u64,
        result: oneshot::Sender<CelebornResult<Vec<(u64, i32)>>>,
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
    },
    MapperEnd {
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    MapperEndComplete {
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
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
    Stop {
        result: oneshot::Sender<()>,
    },
    StopComplete {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for LifecycleManagerMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::GetOrCreateShuffleId { .. } => "GetOrCreateShuffleId",
            Self::GetShuffleIds { .. } => "GetShuffleIds",
            Self::RegisterShuffle { .. } => "RegisterShuffle",
            Self::RegisterShuffleComplete { .. } => "RegisterShuffleComplete",
            Self::MapperEnd { .. } => "MapperEnd",
            Self::MapperEndComplete { .. } => "MapperEndComplete",
            Self::UnregisterShuffle { .. } => "UnregisterShuffle",
            Self::UnregisterShuffleComplete { .. } => "UnregisterShuffleComplete",
            Self::Stop { .. } => "Stop",
            Self::StopComplete { .. } => "StopComplete",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
