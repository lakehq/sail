mod core;
mod handler;
mod local_stream;
mod peer_tracker;
mod rpc;
mod stream_accessor;
mod stream_monitor;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use tokio::sync::oneshot;

use crate::driver::DriverClient;
use crate::id::TaskInstance;
use crate::rpc::ServerMonitor;
use crate::stream::channel::ChannelName;
use crate::worker::actor::local_stream::LocalStream;
use crate::worker::actor::peer_tracker::PeerTracker;
use crate::worker::WorkerOptions;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client: DriverClient,
    peer_tracker: PeerTracker,
    task_signals: HashMap<TaskInstance, oneshot::Sender<()>>,
    local_streams: HashMap<ChannelName, Box<dyn LocalStream>>,
    session_context: Option<Arc<SessionContext>>,
    physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    /// A monotonically increasing sequence number for ordered events.
    sequence: u64,
}
