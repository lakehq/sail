use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::id::{TaskId, WorkerId};
use crate::job::JobDefinition;

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
        result: oneshot::Sender<SendableRecordBatchStream>,
    },
    TaskUpdated {
        worker_id: WorkerId,
        task_id: TaskId,
        status: TaskStatus,
    },
    #[allow(dead_code)]
    Shutdown,
}
