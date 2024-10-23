use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::oneshot;

use crate::id::TaskId;

pub enum WorkerEvent {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    RunTask {
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
    },
    StopTask {
        task_id: TaskId,
        attempt: usize,
    },
    FetchTaskStream {
        task_id: TaskId,
        attempt: usize,
        result: oneshot::Sender<SendableRecordBatchStream>,
    },
    Shutdown,
}
