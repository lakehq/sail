mod core;
mod local;
mod options;

use std::collections::HashMap;

pub use options::StreamManagerOptions;

use crate::id::TaskStreamKey;

pub struct StreamManager {
    options: StreamManagerOptions,
    local_streams: HashMap<TaskStreamKey, Box<dyn local::LocalStream>>,
}
