mod channel;
mod core;
pub(crate) mod memory;
mod options;

use std::collections::HashMap;

pub use options::LocalStreamManagerOptions;
use tokio::sync::oneshot;

use crate::error::ExecutionResult;
use crate::id::TaskStreamKey;
use crate::stream::reader::TaskStreamSource;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

enum LocalStreamState {
    Pending {
        subscribers: Vec<oneshot::Sender<ExecutionResult<TaskStreamSource>>>,
    },
    Created(LocalStream),
    Failed,
}

enum LocalStream {
    Replayable(memory::MemoryStream),
    Single(Option<TaskStreamSource>),
}
