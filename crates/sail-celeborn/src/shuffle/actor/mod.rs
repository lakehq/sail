mod core;
mod handler;
mod options;

use std::collections::HashMap;

pub use options::ShuffleClientOptions;

use crate::master::PartitionLocation;

/// Serializes local shuffle-client operations using an external lifecycle manager.
pub struct ShuffleClientActor {
    options: ShuffleClientOptions,
    locations: HashMap<(i32, i32), PartitionLocation>,
    batch_ids: HashMap<(i32, i32, i32), i32>,
}
