mod core;
mod memory;
mod options;
mod queue;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;

use crate::id::{JobId, TaskStreamKey};

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    /// The memory of the task streams of the process.
    memory: Arc<queue::StreamMemory>,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
    /// The stages whose streams have been removed.
    cleaned: HashSet<(JobId, usize)>,
}

pub enum LocalStreamState {
    Pending {
        waiters: Vec<memory::MemoryStreamWaiter>,
    },
    Created {
        stream: memory::MemoryStream,
    },
    Failed {
        cause: CommonErrorCause,
    },
}
