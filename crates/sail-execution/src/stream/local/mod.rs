mod core;
mod memory;
mod options;

use std::collections::HashMap;

pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;

use crate::id::TaskStreamKey;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<memory::MemoryStreamSender>,
    },
    Created {
        stream: memory::MemoryStream,
    },
    Failed {
        cause: CommonErrorCause,
    },
}
