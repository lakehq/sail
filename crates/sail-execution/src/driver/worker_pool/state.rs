use std::collections::HashSet;

use sail_common::utils::retry::RetrySchedule;
use tokio::time::Instant;

use crate::id::WorkerId;
use crate::worker::WorkerClientSet;

pub struct WorkerDescriptor {
    pub state: WorkerState,
    pub launch: Option<WorkerLaunch>,
    pub messages: Vec<String>,
    /// A list of peer workers known to the worker.
    /// The list may or may not cover all the running workers,
    /// but it does not affect the correctness of the cluster behavior.
    /// The list is only used by the driver to avoid redundant information
    /// when propagating worker locations when running tasks.
    pub peers: HashSet<WorkerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerLaunchReason {
    Initial,
    Demand,
}

#[derive(Debug, Clone)]
pub struct WorkerLaunch {
    pub reason: WorkerLaunchReason,
    /// The launch attempt number. The initial attempt is zero.
    pub attempt: usize,
    pub retries: RetrySchedule,
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
    Completed,
    Failed,
}

impl WorkerState {
    pub fn status(&self) -> &str {
        match self {
            WorkerState::Pending => "PENDING",
            WorkerState::Running { .. } => "RUNNING",
            WorkerState::Completed => "COMPLETED",
            WorkerState::Failed => "FAILED",
        }
    }
}
