use std::collections::HashMap;

use sail_celeborn::lifecycle::LifecycleManagerActor;
use sail_common::actor::ActorHandle;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tonic::async_trait;

use crate::driver::{DriverActor, DriverMessage};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::DriverId;

/// A handle for managing a driver actor.
///
/// This wrapper lets the session manager own the driver lifecycle without exposing
/// [`DriverActor`] or [`DriverMessage`] outside the `sail-execution` crate.
/// Keeping the underlying actor handle private prevents callers from sending arbitrary
/// driver messages and avoids coupling session management to the driver actor implementation.
#[derive(Clone)]
pub struct DriverHandle {
    handle: ActorHandle<DriverActor>,
}

impl DriverHandle {
    pub(crate) fn new(handle: ActorHandle<DriverActor>) -> Self {
        Self { handle }
    }

    pub(crate) async fn send(
        &self,
        message: DriverMessage,
    ) -> Result<(), Box<SendError<DriverMessage>>> {
        self.handle.send(message).await.map_err(Box::new)
    }

    pub(crate) async fn celeborn_lifecycle_manager(
        &self,
    ) -> ExecutionResult<Option<ActorHandle<LifecycleManagerActor>>> {
        let (result, receiver) = oneshot::channel();
        self.send(DriverMessage::CelebornGetLifecycleManager { result })
            .await
            .map_err(ExecutionError::from)?;
        receiver.await.map_err(ExecutionError::from)
    }

    pub async fn activate(&self) -> ExecutionResult<()> {
        self.send(DriverMessage::Activate)
            .await
            .map_err(ExecutionError::from)
    }

    pub async fn shutdown(&self) -> ExecutionResult<()> {
        // A closed channel means that the driver actor has already stopped.
        // Shutdown is intentionally idempotent, so this is still a success.
        let _ = self.send(DriverMessage::Shutdown { result: None }).await;
        Ok(())
    }

    /// Stop the driver and wait for its shutdown hook to complete.
    pub async fn shutdown_and_wait(&self) -> ExecutionResult<()> {
        let (tx, rx) = oneshot::channel();
        if self
            .send(DriverMessage::Shutdown { result: Some(tx) })
            .await
            .is_ok()
        {
            // A closed result channel means another shutdown request won the race.
            // In either case, the driver actor is no longer running.
            let _ = rx.await;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct DriverRegistry {
    drivers: HashMap<DriverId, DriverHandle>,
}

impl DriverRegistry {
    pub fn insert(&mut self, driver_id: DriverId, handle: DriverHandle) -> ExecutionResult<()> {
        if self.drivers.contains_key(&driver_id) {
            return Err(ExecutionError::InternalError(format!(
                "driver {driver_id} is already registered"
            )));
        }
        self.drivers.insert(driver_id, handle);
        Ok(())
    }

    pub fn remove(&mut self, driver_id: DriverId) -> Option<DriverHandle> {
        self.drivers.remove(&driver_id)
    }

    pub fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle> {
        self.drivers
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| ExecutionError::InvalidArgument(format!("driver {driver_id} not found")))
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (DriverId, DriverHandle)> + '_ {
        self.drivers.drain()
    }
}

#[async_trait]
pub trait DriverRegistryAccessor: Send + Sync {
    async fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle>;
}
