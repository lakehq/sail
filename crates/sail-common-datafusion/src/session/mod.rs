mod formatter;
mod runner;

use std::sync::Mutex;

use datafusion::common::Result;
use datafusion_common::internal_datafusion_err;
pub use formatter::PlanFormatter;
pub use runner::JobRunner;
use tokio::time::Instant;

use crate::catalog::display::CatalogDisplay;
use crate::extension::SessionExtension;

pub trait SessionServiceFactory {
    fn create(&self) -> SessionService;
}

pub struct SessionService {
    catalog_command_display: Box<dyn CatalogDisplay>,
    plan_formatter: Box<dyn PlanFormatter>,
    job_runner: Box<dyn JobRunner>,
    /// The time when the Spark session is last seen as active.
    active_at: Mutex<Instant>,
}

impl SessionService {
    pub fn catalog_command_display(&self) -> &dyn CatalogDisplay {
        self.catalog_command_display.as_ref()
    }

    pub fn plan_formatter(&self) -> &dyn PlanFormatter {
        self.plan_formatter.as_ref()
    }

    pub fn job_runner(&self) -> &dyn JobRunner {
        self.job_runner.as_ref()
    }

    pub fn track_activity(&self) -> Result<Instant> {
        let mut active_at = self
            .active_at
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        *active_at = Instant::now();
        Ok(*active_at)
    }

    pub fn active_at(&self) -> Result<Instant> {
        let active_at = self
            .active_at
            .lock()
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(*active_at)
    }
}

impl SessionExtension for SessionService {
    fn name() -> &'static str {
        "SessionService"
    }
}
