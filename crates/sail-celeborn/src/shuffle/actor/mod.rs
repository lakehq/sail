mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

pub use message::ShuffleClientMessage;
pub use options::ShuffleClientOptions;

use crate::common::PartitionLocation;
use crate::worker::WorkerClientPool;

/// Serializes local shuffle-client operations using an external lifecycle manager.
pub struct ShuffleClientActor {
    options: ShuffleClientOptions,
    shuffle_ids: HashMap<(u64, u64), i32>,
    locations: HashMap<(i32, i32), PartitionLocation>,
    location_history: HashMap<(i32, i32), Vec<PartitionLocation>>,
    worker_clients: WorkerClientPool,
    batch_ids: HashMap<(i32, i32, i32), i32>,
}
