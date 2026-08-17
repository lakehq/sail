use datafusion::common::HashMap;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::rpc::ClientOptions;
use crate::worker::peer_tracker::{Peer, PeerTracker, PeerTrackerOptions};
use crate::worker::{WorkerClientSet, WorkerLocation};

impl PeerTracker {
    pub fn new(options: PeerTrackerOptions) -> Self {
        Self {
            options,
            peers: HashMap::new(),
        }
    }

    pub fn track(&mut self, peers: Vec<WorkerLocation>) {
        if peers.is_empty() {
            // Although the logic below can handle empty peer list,
            // we return early as an optimization to avoid unnecessary gRPC calls.
            return;
        }
        for peer in peers {
            self.peers
                .entry(peer.worker_id)
                .or_insert_with(|| Peer::new(peer.host, peer.port));
        }
    }

    pub fn get_client_set(&mut self, worker_id: WorkerId) -> ExecutionResult<WorkerClientSet> {
        if worker_id == self.options.worker_id {
            // Trying to connect to the worker itself via gRPC indicates a bug,
            // so we fail fast here.
            return Err(ExecutionError::InvalidArgument(
                "getting client for the worker itself is not allowed".to_string(),
            ));
        }
        let Some(peer) = self.peers.get_mut(&worker_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "unknown peer worker: {worker_id}"
            )));
        };
        let client = peer.client_set.get_or_insert_with(|| {
            let options = ClientOptions {
                enable_tls: self.options.enable_tls,
                host: peer.host.clone(),
                port: peer.port,
            };
            WorkerClientSet::new(options)
        });
        Ok(client.clone())
    }
}
