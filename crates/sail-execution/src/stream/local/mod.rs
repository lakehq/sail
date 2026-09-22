mod core;
pub(crate) mod memory;
mod options;

use std::collections::HashMap;

pub use options::LocalStreamManagerOptions;

use crate::id::TaskStreamKey;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

struct LocalStreamState {
    stream: memory::MemoryStream,
    pending: bool,
}
