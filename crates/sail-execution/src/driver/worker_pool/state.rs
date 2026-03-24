use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sail_common_datafusion::session::job::WorkerSnapshot;
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
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

impl WorkerDescriptor {
    pub fn worker_snapshot(&self, worker_id: WorkerId) -> WorkerSnapshot {
        let (host, port) = match &self.state {
            WorkerState::Running { host, port, .. } => (Some(host.clone()), Some(*port)),
            _ => (None, None),
        };
        WorkerSnapshot {
            worker_id: worker_id.into(),
            host,
            port,
            status: self.state.status().to_string(),
            created_at: self.created_at,
            stopped_at: self.stopped_at,
        }
    }
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
