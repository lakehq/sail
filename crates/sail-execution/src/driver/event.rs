use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskId, WorkerId};

pub enum DriverEvent {
    ServerReady {
        /// The local port that the driver server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    StartWorker {
        worker_id: WorkerId,
    },
    RegisterWorker {
        worker_id: WorkerId,
        host: String,
        port: u16,
    },
    #[allow(dead_code)]
    StopWorker {
        worker_id: WorkerId,
    },
    ExecuteJob {
        plan: Arc<dyn ExecutionPlan>,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    RemoveJobOutput {
        job_id: JobId,
    },
    UpdateTask {
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
        /// The sequence number from the worker,
        /// or [None] if it is a forced update within the driver.
        sequence: Option<u64>,
    },
    #[allow(dead_code)]
    Shutdown,
}
