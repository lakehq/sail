mod core;
mod local;
mod options;
mod remote;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
pub use options::StreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;

use crate::driver::DriverMessage;
use crate::id::TaskStreamKey;
use crate::stream::error::TaskStreamResult;
use crate::worker::WorkerMessage;

pub struct StreamManager {
    options: StreamManagerOptions,
    remote_streams: Option<Arc<remote::RemoteStreamManager>>,
    local_streams: HashMap<TaskStreamKey, LocalStreamState>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    },
    Created {
        stream: Box<dyn local::LocalStream>,
    },
    Failed {
        cause: CommonErrorCause,
    },
}

pub trait StreamManagerMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self;
}

impl StreamManagerMessage for DriverMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        DriverMessage::ProbePendingLocalStream { key }
    }
}

impl StreamManagerMessage for WorkerMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        WorkerMessage::ProbePendingLocalStream { key }
    }
}
