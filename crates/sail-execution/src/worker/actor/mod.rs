mod core;
mod handler;
mod message;
mod options;
mod rpc;

pub(crate) use message::{WorkerLocation, WorkerMessage, WorkerStreamOwner};
pub(crate) use options::WorkerOptions;

use crate::driver::DriverClientSet;
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;
use crate::worker::peer_tracker::PeerTracker;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client_set: DriverClientSet,
    peer_tracker: PeerTracker,
    task_runner: TaskRunner,
    stream_manager: StreamManager,
    /// A monotonically increasing sequence number for ordered events.
    sequence: u64,
}
