//! Public handle for a system store actor.

use std::path::Path;

use sail_common::actor::{ActorHandle, ActorSystem, UnboundedMailbox};
use tokio::sync::oneshot;

use crate::actor::{SystemStoreActor, SystemStoreMessage};
use crate::backend::{FjallBackend, MemoryBackend};
use crate::engine::{DirectStoreEngine, TransactionalStoreEngine};
use crate::reader::SystemStoreReader;
use crate::{MetricSample, SystemEvent, SystemStoreError, SystemStoreResult};

#[derive(Clone, Debug)]
pub struct SystemStoreHandle {
    inner: SystemStoreHandleInner,
}

#[derive(Clone, Debug)]
pub(crate) enum SystemStoreHandleInner {
    Memory(ActorHandle<SystemStoreActor<DirectStoreEngine<MemoryBackend>>, UnboundedMailbox>),
    Fjall(ActorHandle<SystemStoreActor<TransactionalStoreEngine<FjallBackend>>, UnboundedMailbox>),
}

impl SystemStoreHandleInner {
    pub(crate) fn send(&self, message: SystemStoreMessage) -> SystemStoreResult<()> {
        match self {
            Self::Memory(actor) => actor.send(message),
            Self::Fjall(actor) => actor.send(message),
        }
        .map_err(|error| {
            SystemStoreError::internal(format!("failed to send system store message: {error}"))
        })
    }
}

impl SystemStoreHandle {
    pub fn memory(system: &mut ActorSystem) -> Self {
        Self {
            inner: SystemStoreHandleInner::Memory(system.spawn_unbounded(DirectStoreEngine {
                backend: MemoryBackend::default(),
            })),
        }
    }

    pub fn fjall(system: &mut ActorSystem, path: impl AsRef<Path>) -> SystemStoreResult<Self> {
        Ok(Self {
            inner: SystemStoreHandleInner::Fjall(system.spawn_unbounded(
                TransactionalStoreEngine {
                    backend: FjallBackend::open(path).map_err(SystemStoreError::from)?,
                },
            )),
        })
    }

    pub fn write_event(&self, event: SystemEvent) -> SystemStoreResult<()> {
        self.send(SystemStoreMessage::WriteEvent(event), "event")
    }

    pub async fn write_metrics(&self, samples: Vec<MetricSample>) -> SystemStoreResult<()> {
        let (reply, receiver) = oneshot::channel();
        self.send(
            SystemStoreMessage::WriteMetrics { samples, reply },
            "metrics",
        )?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store metrics cancelled: {error}"))
        })?
    }

    pub fn reader(&self) -> SystemStoreReader {
        SystemStoreReader {
            inner: self.inner.clone(),
        }
    }

    pub async fn flush(&self) -> SystemStoreResult<()> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreMessage::Flush { reply }, "flush")?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store flush cancelled: {error}"))
        })?
    }

    pub async fn shutdown(&self) -> SystemStoreResult<()> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreMessage::Shutdown { reply }, "shutdown")?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store shutdown cancelled: {error}"))
        })?
    }

    fn send(&self, message: SystemStoreMessage, operation: &str) -> SystemStoreResult<()> {
        let result = match &self.inner {
            SystemStoreHandleInner::Memory(actor) => actor.send(message),
            SystemStoreHandleInner::Fjall(actor) => actor.send(message),
        };
        result.map_err(|error| {
            SystemStoreError::internal(format!("failed to send system store {operation}: {error}"))
        })
    }
}
