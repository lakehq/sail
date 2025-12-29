mod core;
mod options;
mod state;

use std::collections::HashMap;
use std::sync::Arc;

pub use options::WorkerPoolOptions;

use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::id::{IdGenerator, TaskKey, WorkerId};
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    driver_server_port: Option<u16>,
    worker_manager: Arc<dyn WorkerManager>,
    workers: HashMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
    /// A lookup table from task attempts to the worker they are assigned to.
    /// This is more convenient than finding the task attempt in the worker descriptors.
    /// Each task attempt can only be assigned to one worker throughout its lifetime.
    /// This lookup table is updated when the task attempt is assigned to a worker,
    /// but there is no need to remove the task attempt when it is completed, as
    /// the mapping is still valid for historical purposes.
    task_assignments: HashMap<TaskKey, WorkerId>,
}

impl WorkerPool {
    pub fn new(worker_manager: Arc<dyn WorkerManager>, options: WorkerPoolOptions) -> Self {
        Self {
            options,
            driver_server_port: None,
            worker_manager,
            workers: HashMap::new(),
            worker_id_generator: IdGenerator::new(),
            task_assignments: HashMap::new(),
        }
    }
}

pub enum WorkerTimeout {
    Yes,
    No,
}

pub enum WorkerIdle {
    Yes,
    No,
}

pub enum WorkerLost {
    Yes,
    No,
}
