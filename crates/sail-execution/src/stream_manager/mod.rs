mod core;
mod local;
mod options;

use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
pub use options::StreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;

use crate::driver::DriverEvent;
use crate::id::TaskStreamKey;
use crate::stream::error::TaskStreamResult;
use crate::worker::WorkerEvent;

pub struct StreamManager {
    options: StreamManagerOptions,
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

impl StreamManagerMessage for DriverEvent {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        DriverEvent::ProbePendingLocalStream { key }
    }
}

impl StreamManagerMessage for WorkerEvent {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        WorkerEvent::ProbePendingLocalStream { key }
    }
}
