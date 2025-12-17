mod core;
mod options;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
pub use options::WorkerPoolOptions;
use tokio::time::Instant;

use crate::codec::RemoteExecutionCodec;
use crate::id::{IdGenerator, JobId, TaskInstance, WorkerId};
use crate::worker::WorkerClient;
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

struct WorkerDescriptor {
    state: WorkerState,
    messages: Vec<String>,
    /// A list of peer workers known to the worker.
    /// The list may or may not cover all the running workers,
    /// but it does not affect the correctness of the cluster behavior.
    /// The list is only used by the driver to avoid redundant information
    /// when propagating worker locations when running tasks.
    peers: HashSet<WorkerId>,
}

enum WorkerState {
    Pending,
    Running {
        host: String,
        port: u16,
        /// The tasks that are running on the worker.
        tasks: HashSet<TaskInstance>,
        /// The jobs that depend on the worker.
        /// This is used to support a naive version of the Spark "shuffle tracking" mechanism.
        /// A job depends on a worker if the tasks of the job are running on the worker,
        /// or if the worker owns a channel for the job output.
        /// The worker needs to be running for shuffle stream or job output stream consumption.
        jobs: HashSet<JobId>,
        updated_at: Instant,
        heartbeat_at: Instant,
        /// The gRPC client to communicate with the worker if the connection is established.
        client: Option<WorkerClient>,
    },
    Stopped,
    Failed,
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
