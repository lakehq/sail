use std::collections::HashSet;

use tokio::time::Instant;

use crate::id::WorkerId;
use crate::worker::WorkerClientSet;

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
        updated_at: Instant,
        heartbeat_at: Instant,
        /// The gRPC client to communicate with the worker if the connection is established.
        client: Option<WorkerClientSet>,
    },
    Stopped,
    Failed,
}
