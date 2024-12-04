use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::oneshot;
use tokio::time::Instant;

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
    RegisterWorker {
        worker_id: WorkerId,
        host: String,
        port: u16,
        result: oneshot::Sender<ExecutionResult<()>>,
    },
    WorkerHeartbeat {
        worker_id: WorkerId,
    },
    ProbePendingWorker {
        worker_id: WorkerId,
    },
    ProbeIdleWorker {
        worker_id: WorkerId,
        instant: Instant,
    },
    ProbeLostWorker {
        worker_id: WorkerId,
        instant: Instant,
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
    ProbePendingTask {
        task_id: TaskId,
        attempt: usize,
    },
    Shutdown,
}
