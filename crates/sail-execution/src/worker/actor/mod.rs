mod core;
mod handler;
mod peer_tracker;
mod rpc;
mod stream_accessor;
mod task_monitor;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use tokio::sync::oneshot;

use crate::driver::DriverClientSet;
use crate::id::TaskKey;
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::worker::actor::peer_tracker::PeerTracker;
use crate::worker::WorkerOptions;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client_set: DriverClientSet,
    peer_tracker: PeerTracker,
    task_signals: HashMap<TaskKey, oneshot::Sender<()>>,
    stream_manager: StreamManager,
    physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    /// A monotonically increasing sequence number for ordered events.
    sequence: u64,
}
