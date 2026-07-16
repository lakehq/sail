use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use sail_server::actor::ActorHandle;
use tokio::sync::mpsc::error::SendError;

use crate::driver::{DriverActor, DriverEvent};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::DriverId;

/// A handle for managing a driver actor.
///
/// This wrapper lets the session manager own the driver lifecycle without exposing
/// [`DriverActor`] or [`DriverEvent`] outside the `sail-execution` crate.
/// Keeping the underlying actor handle private prevents callers from sending arbitrary
/// driver events and avoids coupling session management to the driver actor implementation.
#[derive(Clone)]
pub struct DriverHandle {
    handle: ActorHandle<DriverActor>,
}

impl DriverHandle {
    pub(crate) fn new(handle: ActorHandle<DriverActor>) -> Self {
        Self { handle }
    }

    pub(crate) async fn send(&self, event: DriverEvent) -> Result<(), SendError<DriverEvent>> {
        self.handle.send(event).await
    }

    pub async fn activate(&self) -> ExecutionResult<()> {
        self.send(DriverEvent::Activate)
            .await
            .map_err(ExecutionError::from)
    }

    pub async fn shutdown(&self) -> ExecutionResult<()> {
        self.send(DriverEvent::Shutdown { history: None })
            .await
            .map_err(ExecutionError::from)
    }
}

#[derive(Clone, Default)]
pub struct DriverRegistry {
    drivers: Arc<RwLock<HashMap<DriverId, DriverHandle>>>,
}

impl DriverRegistry {
    pub fn insert(&self, driver_id: DriverId, handle: DriverHandle) -> ExecutionResult<()> {
        let mut drivers = self
            .drivers
            .write()
            .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
        if drivers.contains_key(&driver_id) {
            return Err(ExecutionError::InternalError(format!(
                "driver {driver_id} is already registered"
            )));
        }
        drivers.insert(driver_id, handle);
        Ok(())
    }

    pub fn remove(&self, driver_id: DriverId) -> ExecutionResult<Option<DriverHandle>> {
        let mut drivers = self
            .drivers
            .write()
            .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
        Ok(drivers.remove(&driver_id))
    }

    pub(crate) fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle> {
        let drivers = self
            .drivers
            .read()
            .map_err(|e| ExecutionError::InternalError(e.to_string()))?;
        drivers
            .get(&driver_id)
            .cloned()
            .ok_or_else(|| ExecutionError::InvalidArgument(format!("driver {driver_id} not found")))
    }
}
