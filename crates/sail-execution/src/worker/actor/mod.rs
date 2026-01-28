mod core;
mod handler;
mod rpc;

use crate::driver::DriverClientSet;
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;
use crate::worker::peer_tracker::PeerTracker;
use crate::worker::WorkerOptions;

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
