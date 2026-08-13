mod core;
mod handler;
mod message;
mod options;

use std::collections::HashMap;

pub use message::ShuffleClientMessage;
pub use options::ShuffleClientOptions;

use crate::master::PartitionLocation;
use crate::worker::WorkerClient;

/// Serializes local shuffle-client operations using an external lifecycle manager.
pub struct ShuffleClientActor {
    options: ShuffleClientOptions,
    shuffle_ids: HashMap<(u64, u64), i32>,
    locations: HashMap<(i32, i32), PartitionLocation>,
    location_history: HashMap<(i32, i32), Vec<PartitionLocation>>,
    worker_clients: HashMap<(i32, i32), WorkerClient>,
    batch_ids: HashMap<(i32, i32, i32), i32>,
}
