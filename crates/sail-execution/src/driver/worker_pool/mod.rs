mod core;
mod observer;
mod options;
mod state;

use std::sync::Arc;

use indexmap::IndexMap;
pub use options::WorkerPoolOptions;

use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::id::{IdGenerator, WorkerId};
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    driver_server_port: Option<u16>,
    worker_manager: Arc<dyn WorkerManager>,
    workers: IndexMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
}

impl WorkerPool {
    pub fn new(worker_manager: Arc<dyn WorkerManager>, options: WorkerPoolOptions) -> Self {
        Self {
            options,
            driver_server_port: None,
            worker_manager,
            workers: IndexMap::new(),
            worker_id_generator: IdGenerator::new(),
        }
    }
}
