use std::collections::HashSet;

use tokio::time::Instant;

use crate::id::{JobId, TaskInstance, WorkerId};
use crate::worker::WorkerClient;

pub struct WorkerDescriptor {
    pub state: WorkerState,
    pub messages: Vec<String>,
    /// A list of peer workers known to the worker.
    /// The list may or may not cover all the running workers,
    /// but it does not affect the correctness of the cluster behavior.
    /// The list is only used by the driver to avoid redundant information
    /// when propagating worker locations when running tasks.
    pub peers: HashSet<WorkerId>,
}

pub enum WorkerState {
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
