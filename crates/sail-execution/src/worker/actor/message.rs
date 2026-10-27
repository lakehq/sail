use std::borrow::Cow;

use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;

use crate::error::ExecutionError;
use crate::id::WorkerId;
use crate::worker::r#gen;

pub enum WorkerMessage {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    StartHeartbeat,
    Shutdown,
}

impl SpanAssociation for WorkerMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::ServerReady { .. } => "ServerReady",
            Self::StartHeartbeat => "StartHeartbeat",
            Self::Shutdown => "Shutdown",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut properties: Vec<(&'static str, String)> = vec![];
        match self {
            Self::ServerReady { port, signal: _ } => {
                properties.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
            }
            Self::StartHeartbeat | Self::Shutdown => {}
        }
        properties
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
    }
}

pub struct WorkerLocation {
    pub worker_id: WorkerId,
    pub host: String,
    pub port: u16,
}

impl From<WorkerLocation> for r#gen::WorkerLocation {
    fn from(value: WorkerLocation) -> Self {
        Self {
            worker_id: value.worker_id.into(),
            host: value.host,
            port: value.port as u32,
        }
    }
}

impl TryFrom<r#gen::WorkerLocation> for WorkerLocation {
    type Error = ExecutionError;

    fn try_from(value: r#gen::WorkerLocation) -> Result<Self, Self::Error> {
        let port = u16::try_from(value.port).map_err(|_| {
            ExecutionError::InvalidArgument(format!("invalid port: {}", value.port))
        })?;
        Ok(Self {
            worker_id: value.worker_id.into(),
            host: value.host,
            port,
        })
    }
}
