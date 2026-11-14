mod channel;
mod core;
#[cfg(test)]
mod gate_model;
mod memory;
mod options;

use std::collections::{HashMap, HashSet};

pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;

use crate::id::{JobId, TaskStreamKey};

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
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
