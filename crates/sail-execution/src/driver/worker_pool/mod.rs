mod core;
mod options;
mod state;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub use options::WorkerPoolOptions;

use crate::codec::RemoteExecutionCodec;
use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::id::{IdGenerator, WorkerId};
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    driver_server_port: Option<u16>,
    worker_manager: Arc<dyn WorkerManager>,
    workers: HashMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
    physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
}

impl WorkerPool {
    pub fn new(worker_manager: Arc<dyn WorkerManager>, options: WorkerPoolOptions) -> Self {
        Self {
            options,
            driver_server_port: None,
            worker_manager,
            workers: HashMap::new(),
            worker_id_generator: IdGenerator::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec),
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
