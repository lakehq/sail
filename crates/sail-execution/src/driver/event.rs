use std::borrow::Cow;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::session::job::JobRunnerHistory;
use sail_common_datafusion::system::observable::JobRunnerObserver;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::driver::gen;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};

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
    ProbePendingLocalStream {
        key: TaskStreamKey,
    },
    CreateLocalStream {
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    },
    CreateRemoteStream {
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
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
    FetchRemoteStream {
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    ObserveState {
        observer: JobRunnerObserver,
    },
    Shutdown {
        history: Option<oneshot::Sender<JobRunnerHistory>>,
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

impl From<gen::TaskStatus> for TaskStatus {
    fn from(value: gen::TaskStatus) -> Self {
        match value {
            gen::TaskStatus::Running => Self::Running,
            gen::TaskStatus::Succeeded => Self::Succeeded,
            gen::TaskStatus::Failed => Self::Failed,
            gen::TaskStatus::Canceled => Self::Canceled,
        }
    }
}

impl From<TaskStatus> for gen::TaskStatus {
    fn from(value: TaskStatus) -> Self {
        match value {
            TaskStatus::Running => gen::TaskStatus::Running,
            TaskStatus::Succeeded => gen::TaskStatus::Succeeded,
            TaskStatus::Failed => gen::TaskStatus::Failed,
            TaskStatus::Canceled => gen::TaskStatus::Canceled,
        }
    }
}

impl SpanAssociation for DriverEvent {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            DriverEvent::ServerReady { .. } => "ServerReady",
            DriverEvent::RegisterWorker { .. } => "RegisterWorker",
            DriverEvent::WorkerHeartbeat { .. } => "WorkerHeartbeat",
            DriverEvent::WorkerKnownPeers { .. } => "WorkerKnownPeers",
            DriverEvent::ProbePendingWorker { .. } => "ProbePendingWorker",
            DriverEvent::ProbeIdleWorker { .. } => "ProbeIdleWorker",
            DriverEvent::ProbeLostWorker { .. } => "ProbeLostWorker",
            DriverEvent::ExecuteJob { .. } => "ExecuteJob",
            DriverEvent::CleanUpJob { .. } => "CleanUpJob",
            DriverEvent::UpdateTask { .. } => "UpdateTask",
            DriverEvent::ProbePendingTask { .. } => "ProbePendingTask",
            DriverEvent::ProbePendingLocalStream { .. } => "ProbePendingLocalStream",
            DriverEvent::CreateLocalStream { .. } => "CreateLocalStream",
            DriverEvent::CreateRemoteStream { .. } => "CreateRemoteStream",
            DriverEvent::FetchDriverStream { .. } => "FetchDriverStream",
            DriverEvent::FetchWorkerStream { .. } => "FetchWorkerStream",
            DriverEvent::FetchRemoteStream { .. } => "FetchRemoteStream",
            DriverEvent::ObserveState { .. } => "ObserveState",
            DriverEvent::Shutdown { .. } => "Shutdown",
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
            | DriverEvent::WorkerKnownPeers {
                worker_id,
                peer_worker_ids: _,
            }
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
            DriverEvent::ExecuteJob {
                plan: _,
                context: _,
                result: _,
            } => {}
            DriverEvent::CleanUpJob { job_id } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
            }
            DriverEvent::UpdateTask {
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
            DriverEvent::ProbePendingTask {
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
            DriverEvent::ProbePendingLocalStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            DriverEvent::CreateLocalStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                storage,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
                p.push((
                    SpanAttribute::EXECUTION_STREAM_LOCAL_STORAGE,
                    storage.to_string(),
                ));
            }
            DriverEvent::CreateRemoteStream {
                uri,
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
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
                p.push((SpanAttribute::EXECUTION_STREAM_REMOTE_URI, uri.clone()));
            }
            DriverEvent::FetchDriverStream {
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
            DriverEvent::FetchWorkerStream {
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
            DriverEvent::FetchRemoteStream {
                uri,
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
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
                p.push((SpanAttribute::EXECUTION_STREAM_REMOTE_URI, uri.clone()));
            }
            DriverEvent::ObserveState { observer: _ } => {}
            DriverEvent::Shutdown { .. } => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
