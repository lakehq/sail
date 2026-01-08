use datafusion::common::HashMap;
use sail_server::actor::ActorContext;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::rpc::ClientOptions;
use crate::worker::{WorkerActor, WorkerClient, WorkerEvent, WorkerLocation, WorkerOptions};

#[readonly::make]
pub struct PeerTrackerOptions {
    pub enable_tls: bool,
}

impl PeerTrackerOptions {
    pub fn new(options: &WorkerOptions) -> Self {
        Self {
            enable_tls: options.enable_tls,
        }
    }
}

/// A utility to track peer workers and manage gRPC clients to them.
/// This does not remove peers when they are gone.
/// It is the responsibility of the driver to ensure that the worker
/// only contacts active peers when running tasks.
pub struct PeerTracker {
    options: PeerTrackerOptions,
    peers: HashMap<WorkerId, Peer>,
}

impl PeerTracker {
    pub fn new(options: PeerTrackerOptions) -> Self {
        Self {
            options,
            peers: HashMap::new(),
        }
    }

    pub fn track(&mut self, ctx: &mut ActorContext<WorkerActor>, peers: Vec<WorkerLocation>) {
        if peers.is_empty() {
            // Although the logic below can handle empty peer list,
            // we return early as an optimization to avoid unnecessary gRPC calls.
            return;
        }
        let peer_worker_ids = peers.iter().map(|x| x.worker_id).collect();
        for peer in peers {
            self.peers
                .entry(peer.worker_id)
                .or_insert_with(|| Peer::new(peer.host, peer.port));
        }
        ctx.send(WorkerEvent::ReportKnownPeers { peer_worker_ids });
    }

    pub fn get_client(&mut self, worker_id: WorkerId) -> ExecutionResult<WorkerClient> {
        let Some(peer) = self.peers.get_mut(&worker_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "unknown peer worker: {worker_id}"
            )));
        };
        let client = peer.client.get_or_insert_with(|| {
            let options = ClientOptions {
                enable_tls: self.options.enable_tls,
                host: peer.host.clone(),
                port: peer.port,
            };
            WorkerClient::new(options)
        });
        Ok(client.clone())
    }
}

struct Peer {
    host: String,
    port: u16,
    client: Option<WorkerClient>,
}

impl Peer {
    fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            client: None,
        }
    }
}
