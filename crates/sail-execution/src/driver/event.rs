use std::borrow::Cow;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::error::CommonErrorCause;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
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
    CleanUpJob {
        job_id: JobId,
    },
    UpdateTask {
        task_id: TaskId,
        attempt: usize,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
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

impl SpanAssociation for DriverEvent {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            DriverEvent::ServerReady { .. } => "ServerReady",
            DriverEvent::RegisterWorker { .. } => "RegisterWorker",
            DriverEvent::WorkerHeartbeat { .. } => "WorkerHeartbeat",
            DriverEvent::ProbePendingWorker { .. } => "ProbePendingWorker",
            DriverEvent::ProbeIdleWorker { .. } => "ProbeIdleWorker",
            DriverEvent::ProbeLostWorker { .. } => "ProbeLostWorker",
            DriverEvent::ExecuteJob { .. } => "ExecuteJob",
            DriverEvent::CleanUpJob { .. } => "CleanUpJob",
            DriverEvent::UpdateTask { .. } => "UpdateTask",
            DriverEvent::ProbePendingTask { .. } => "ProbePendingTask",
            DriverEvent::Shutdown => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            DriverEvent::ServerReady { port, signal: _ } => {
                p.push((SpanAttribute::CLUSTER_DRIVER_PORT, port.to_string()));
            }
            DriverEvent::RegisterWorker {
                worker_id,
                host,
                port,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                p.push((SpanAttribute::CLUSTER_WORKER_HOST, host.clone()));
                p.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
            }
            DriverEvent::WorkerHeartbeat { worker_id }
            | DriverEvent::ProbePendingWorker { worker_id }
            | DriverEvent::ProbeIdleWorker {
                worker_id,
                instant: _,
            }
            | DriverEvent::ProbeLostWorker {
                worker_id,
                instant: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
            }
            DriverEvent::ExecuteJob { plan: _, result: _ } => {}
            DriverEvent::CleanUpJob { job_id } => {
                p.push((SpanAttribute::CLUSTER_JOB_ID, job_id.to_string()));
            }
            DriverEvent::UpdateTask {
                task_id,
                attempt,
                status,
                message,
                cause,
                sequence: _,
            } => {
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_STATUS, status.to_string()));
                if let Some(message) = message {
                    p.push((SpanAttribute::CLUSTER_TASK_MESSAGE, message.clone()));
                }
                if let Some(cause) = cause {
                    p.push((
                        SpanAttribute::CLUSTER_TASK_ERROR_CAUSE,
                        format!("{cause:?}"),
                    ));
                }
            }
            DriverEvent::ProbePendingTask { task_id, attempt } => {
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
            }
            DriverEvent::Shutdown => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
