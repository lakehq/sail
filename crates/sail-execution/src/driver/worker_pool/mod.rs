mod core;
mod options;
mod state;

use std::sync::Arc;

use indexmap::IndexMap;
pub use options::WorkerPoolOptions;
use sail_telemetry::events::SystemEventReporter;
pub(crate) use state::{WorkerLaunch, WorkerLaunchReason};

use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::id::{IdGenerator, WorkerId};
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    worker_manager: Arc<dyn WorkerManager>,
    workers: IndexMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
    event_reporter: SystemEventReporter,
}

impl WorkerPool {
    pub fn new(
        worker_manager: Box<dyn WorkerManager>,
        options: WorkerPoolOptions,
        event_reporter: SystemEventReporter,
    ) -> Self {
        Self {
            options,
            worker_manager: Arc::from(worker_manager),
            workers: IndexMap::new(),
            worker_id_generator: IdGenerator::new(),
            event_reporter,
        }
    }
}
