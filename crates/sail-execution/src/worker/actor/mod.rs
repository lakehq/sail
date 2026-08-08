mod core;
mod extensions;
mod handler;
mod message;
mod options;
mod rpc;

use extensions::WorkerExtensions;
pub(crate) use message::{WorkerLocation, WorkerMessage, WorkerStreamOwner};
pub(crate) use options::WorkerOptions;

use crate::driver::DriverClientSet;
use crate::rpc::ServerMonitor;
use crate::task_runner::TaskRunner;
use crate::worker::peer_tracker::PeerTracker;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client_set: DriverClientSet,
    peer_tracker: PeerTracker,
    task_runner: TaskRunner,
    extensions: WorkerExtensions,
    /// A monotonically increasing sequence number for ordered messages.
    sequence: u64,
}
