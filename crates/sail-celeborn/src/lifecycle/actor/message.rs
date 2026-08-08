use std::borrow::Cow;

use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::error::CelebornResult;
use crate::master::SlotReservation;

pub enum LifecycleManagerMessage {
    RequestSlotsBegin {
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        should_replicate: bool,
        max_workers: i32,
        result: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    RequestSlotsEnd {
        shuffle_id: i32,
        result: CelebornResult<SlotReservation>,
        reply: oneshot::Sender<CelebornResult<SlotReservation>>,
    },
    MapperEndBegin {
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    MapperEndCommitEnd {
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    },
    UnregisterShuffleBegin {
        shuffle_id: i32,
        result: oneshot::Sender<CelebornResult<()>>,
    },
    UnregisterShuffleEnd {
        shuffle_id: i32,
        result: CelebornResult<()>,
        reply: oneshot::Sender<CelebornResult<()>>,
    },
    Stop {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for LifecycleManagerMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::RequestSlotsBegin { .. } => "RequestSlotsBegin",
            Self::RequestSlotsEnd { .. } => "RequestSlotsEnd",
            Self::MapperEndBegin { .. } => "MapperEndBegin",
            Self::MapperEndCommitEnd { .. } => "MapperEndCommitEnd",
            Self::UnregisterShuffleBegin { .. } => "UnregisterShuffleBegin",
            Self::UnregisterShuffleEnd { .. } => "UnregisterShuffleEnd",
            Self::Stop { .. } => "Stop",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
