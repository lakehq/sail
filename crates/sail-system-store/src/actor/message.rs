use std::borrow::Cow;

use sail_common::telemetry::SpanAssociation;
use tokio::sync::oneshot;

use crate::{MetricSample, SystemEvent, SystemStoreQuery, SystemStoreResult};

pub(crate) enum SystemStoreMessage {
    WriteEvent(SystemEvent),
    WriteMetrics {
        samples: Vec<MetricSample>,
        reply: oneshot::Sender<SystemStoreResult<()>>,
    },
    Read(SystemStoreQuery),
    Flush {
        reply: oneshot::Sender<SystemStoreResult<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<SystemStoreResult<()>>,
    },
}

impl SpanAssociation for SystemStoreMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::WriteEvent(_) => "WriteEvent",
            Self::WriteMetrics { .. } => "WriteMetrics",
            Self::Read(_) => "Read",
            Self::Flush { .. } => "Flush",
            Self::Shutdown { .. } => "Shutdown",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
