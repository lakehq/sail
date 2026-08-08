pub(crate) mod accessor;
mod core;
mod memory;
mod options;

use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;

use crate::driver::DriverMessage;
use crate::id::TaskStreamKey;
use crate::stream::error::TaskStreamResult;
use crate::worker::WorkerMessage;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    },
    Created {
        stream: memory::MemoryStream,
    },
    Failed {
        cause: CommonErrorCause,
    },
}

pub trait LocalStreamManagerMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self;
}

impl LocalStreamManagerMessage for DriverMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        DriverMessage::ProbePendingLocalStream { key }
    }
}

impl LocalStreamManagerMessage for WorkerMessage {
    fn probe_pending_local_stream(key: TaskStreamKey) -> Self {
        WorkerMessage::ProbePendingLocalStream { key }
    }
}
