use std::sync::Mutex;

use datafusion_common::internal_datafusion_err;
use tokio::time::Instant;

use crate::extension::SessionExtension;

pub struct ActivityTracker {
    /// The time when the Spark session is last seen as active.
    active_at: Mutex<Instant>,
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ActivityTracker {
    pub fn new() -> Self {
        Self {
            active_at: Mutex::new(Instant::now()),
        }
    }

    pub fn track_activity(&self) -> datafusion_common::Result<Instant> {
        let mut active_at = self
            .active_at
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        *active_at = Instant::now();
        Ok(*active_at)
    }

    pub fn active_at(&self) -> datafusion_common::Result<Instant> {
        let active_at = self
            .active_at
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(*active_at)
    }
}

impl SessionExtension for ActivityTracker {
    fn name() -> &'static str {
        "ActivityTracker"
    }
}
