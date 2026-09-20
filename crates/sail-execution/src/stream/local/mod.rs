mod core;
mod memory;
mod options;

use std::collections::HashMap;

pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::id::TaskStreamKey;
use crate::stream::error::TaskStreamResult;
use crate::stream::reader::TaskStreamSource;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<oneshot::Sender<TaskStreamResult<TaskStreamSource>>>,
    },
    Created {
        stream: memory::MemoryStream,
    },
    Failed {
        cause: CommonErrorCause,
    },
}
