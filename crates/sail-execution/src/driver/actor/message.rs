use std::borrow::Cow;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use sail_celeborn::lifecycle::LifecycleManagerActor;
use sail_common::actor::ActorHandle;
use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::system::observable::JobRunnerObserver;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::r#gen;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;

pub enum DriverMessage {
    Activate,
    RegisterWorker {
        worker_id: WorkerId,
        host: String,
        port: u16,
        result: oneshot::Sender<ExecutionResult<()>>,
    },
    WorkerHeartbeat {
        worker_id: WorkerId,
    },
    WorkerKnownPeers {
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
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
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    CleanUpJob {
        job_id: JobId,
    },
    UpdateTask {
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        /// The sequence number from the worker,
        /// or [None] if it is a forced update within the driver.
        sequence: Option<u64>,
    },
    ProbePendingTask {
        key: TaskKey,
    },
    FetchDriverStream {
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchWorkerStream {
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    CelebornGetLifecycleManager {
        result: oneshot::Sender<Option<ActorHandle<LifecycleManagerActor>>>,
    },
    ObserveState {
        observer: JobRunnerObserver,
    },
    Shutdown {
        result: Option<oneshot::Sender<()>>,
    },
}

/// The observed task status that drives the task state transition.
#[derive(Debug, Clone, Copy)]
pub enum TaskStatus {
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TaskStatus::Running => write!(f, "RUNNING"),
            TaskStatus::Succeeded => write!(f, "SUCCEEDED"),
            TaskStatus::Failed => write!(f, "FAILED"),
            TaskStatus::Canceled => write!(f, "CANCELED"),
        }
    }
}

impl From<r#gen::TaskStatus> for TaskStatus {
    fn from(value: r#gen::TaskStatus) -> Self {
        match value {
            r#gen::TaskStatus::Running => Self::Running,
            r#gen::TaskStatus::Succeeded => Self::Succeeded,
            r#gen::TaskStatus::Failed => Self::Failed,
            r#gen::TaskStatus::Canceled => Self::Canceled,
        }
    }
}

impl From<TaskStatus> for r#gen::TaskStatus {
    fn from(value: TaskStatus) -> Self {
        match value {
            TaskStatus::Running => r#gen::TaskStatus::Running,
            TaskStatus::Succeeded => r#gen::TaskStatus::Succeeded,
            TaskStatus::Failed => r#gen::TaskStatus::Failed,
            TaskStatus::Canceled => r#gen::TaskStatus::Canceled,
        }
    }
}

impl SpanAssociation for DriverMessage {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            DriverMessage::Activate => "Activate",
            DriverMessage::RegisterWorker { .. } => "RegisterWorker",
            DriverMessage::WorkerHeartbeat { .. } => "WorkerHeartbeat",
            DriverMessage::WorkerKnownPeers { .. } => "WorkerKnownPeers",
            DriverMessage::ProbePendingWorker { .. } => "ProbePendingWorker",
            DriverMessage::ProbeIdleWorker { .. } => "ProbeIdleWorker",
            DriverMessage::ProbeLostWorker { .. } => "ProbeLostWorker",
            DriverMessage::ExecuteJob { .. } => "ExecuteJob",
            DriverMessage::CleanUpJob { .. } => "CleanUpJob",
            DriverMessage::UpdateTask { .. } => "UpdateTask",
            DriverMessage::ProbePendingTask { .. } => "ProbePendingTask",
            DriverMessage::FetchDriverStream { .. } => "FetchDriverStream",
            DriverMessage::FetchWorkerStream { .. } => "FetchWorkerStream",
            DriverMessage::CelebornGetLifecycleManager { .. } => "CelebornGetLifecycleManager",
            DriverMessage::ObserveState { .. } => "ObserveState",
            DriverMessage::Shutdown { .. } => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            DriverMessage::Activate => {}
            DriverMessage::RegisterWorker {
                worker_id,
                host,
                port,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                p.push((SpanAttribute::CLUSTER_WORKER_HOST, host.clone()));
                p.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
            }
            DriverMessage::WorkerHeartbeat { worker_id }
            | DriverMessage::WorkerKnownPeers {
                worker_id,
                peer_worker_ids: _,
            }
            | DriverMessage::ProbePendingWorker { worker_id }
            | DriverMessage::ProbeIdleWorker {
                worker_id,
                instant: _,
            }
            | DriverMessage::ProbeLostWorker {
                worker_id,
                instant: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
            }
            DriverMessage::ExecuteJob {
                plan: _,
                context: _,
                result: _,
            } => {}
            DriverMessage::CleanUpJob { job_id } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
            }
            DriverMessage::UpdateTask {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
                status,
                message,
                cause,
                sequence: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_TASK_STATUS, status.to_string()));
                if let Some(message) = message {
                    p.push((SpanAttribute::EXECUTION_TASK_MESSAGE, message.clone()));
                }
                if let Some(cause) = cause {
                    p.push((
                        SpanAttribute::EXECUTION_TASK_ERROR_CAUSE,
                        format!("{cause:?}"),
                    ));
                }
            }
            DriverMessage::ProbePendingTask {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
            }
            DriverMessage::FetchDriverStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                result: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            DriverMessage::FetchWorkerStream {
                worker_id,
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            DriverMessage::CelebornGetLifecycleManager { result: _ } => {}
            DriverMessage::ObserveState { observer: _ } => {}
            DriverMessage::Shutdown { .. } => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
