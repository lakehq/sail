mod core;
mod options;

use datafusion::common::HashMap;
pub use options::PeerTrackerOptions;

use crate::id::WorkerId;
use crate::worker::WorkerClientSet;

/// A utility to track peer workers and manage gRPC clients to them.
/// This does not remove peers when they are gone.
/// It is the responsibility of the driver to ensure that the worker
/// only contacts active peers when running tasks.
pub struct PeerTracker {
    options: PeerTrackerOptions,
    peers: HashMap<WorkerId, Peer>,
}

struct Peer {
    host: String,
    port: u16,
    client_set: Option<WorkerClientSet>,
}

impl Peer {
    fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            client_set: None,
        }
    }
}
