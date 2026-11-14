mod core;
mod memory;
mod options;
mod shared;

use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
pub use options::LocalStreamManagerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::mpsc;

use crate::id::{JobId, TaskStreamKey};
use crate::stream::error::TaskStreamResult;

pub struct LocalStreamManager {
    options: LocalStreamManagerOptions,
    streams: HashMap<TaskStreamKey, LocalStreamState>,
    /// The stages of jobs whose streams are removed, since every consumer of the
    /// streams has finished, and the latest attempt of each partition of the stage when
    /// it was removed. A stream of such an attempt or an earlier one that is created later
    /// is not read.
    removed: HashMap<(JobId, usize), Vec<usize>>,
}

pub enum LocalStreamState {
    Pending {
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    },
    Created {
        stream: memory::MemoryStream,
    },
    Shared {
        stream: shared::SharedStream,
    },
    Failed {
        cause: CommonErrorCause,
    },
}
