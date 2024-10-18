use arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use tokio::sync::{mpsc, oneshot};

use crate::driver::state::TaskStatus;
use crate::id::{TaskId, WorkerId};
use crate::job::JobDefinition;

pub type TaskChannel = mpsc::Sender<Result<RecordBatch, DataFusionError>>;

pub enum DriverEvent {
    ServerReady {
        /// The local port that the driver server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    RegisterWorker {
        worker_id: WorkerId,
        host: String,
        port: u16,
    },
    ExecuteJob {
        job: JobDefinition,
        channel: TaskChannel,
    },
    TaskUpdated {
        task_id: TaskId,
        partition: usize,
        status: TaskStatus,
    },
    #[allow(dead_code)]
    Shutdown,
}
