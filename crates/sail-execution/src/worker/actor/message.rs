use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task::definition::TaskDefinition;
use crate::worker::r#gen;

pub enum WorkerMessage {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    StartHeartbeat,
    ReportKnownPeers {
        peer_worker_ids: Vec<WorkerId>,
    },
    RunTask {
        key: TaskKey,
        definition: TaskDefinition,
        peers: Vec<WorkerLocation>,
    },
    StopTask {
        key: TaskKey,
    },
    ReportTaskStatus {
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    },
    ProbePendingLocalStream {
        key: TaskStreamKey,
    },
    CreateLocalStream {
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    },
    CreateStorageStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    },
    FetchDriverStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchWorkerStream {
        owner: WorkerStreamOwner,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchStorageStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    CleanUpJob {
        job_id: JobId,
        stage: Option<usize>,
    },
    Shutdown,
}

pub enum WorkerStreamOwner {
    This,
    Worker {
        worker_id: WorkerId,
        schema: SchemaRef,
    },
}

impl SpanAssociation for WorkerMessage {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            WorkerMessage::ServerReady { .. } => "ServerReady",
            WorkerMessage::StartHeartbeat => "StartHeartbeat",
            WorkerMessage::ReportKnownPeers { .. } => "ReportKnownPeers",
            WorkerMessage::RunTask { .. } => "RunTask",
            WorkerMessage::StopTask { .. } => "StopTask",
            WorkerMessage::ReportTaskStatus { .. } => "ReportTaskStatus",
            WorkerMessage::ProbePendingLocalStream { .. } => "ProbePendingLocalStream",
            WorkerMessage::CreateLocalStream { .. } => "CreateLocalStream",
            WorkerMessage::CreateStorageStream { .. } => "CreateStorageStream",
            WorkerMessage::FetchDriverStream { .. } => "FetchDriverStream",
            WorkerMessage::FetchWorkerStream { .. } => "FetchWorkerStream",
            WorkerMessage::FetchStorageStream { .. } => "FetchStorageStream",
            WorkerMessage::CleanUpJob { .. } => "CleanUpJob",
            WorkerMessage::Shutdown => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            WorkerMessage::ServerReady { port, signal: _ } => {
                p.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
            }
            WorkerMessage::StartHeartbeat => {}
            WorkerMessage::ReportKnownPeers { peer_worker_ids: _ } => {}
            WorkerMessage::RunTask {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
                definition: _,
                peers: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
            }
            WorkerMessage::StopTask {
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
            WorkerMessage::ReportTaskStatus {
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
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_TASK_STATUS, status.to_string()));
                if let Some(msg) = message {
                    p.push((SpanAttribute::EXECUTION_TASK_MESSAGE, msg.clone()));
                }
                if let Some(cause) = cause {
                    p.push((
                        SpanAttribute::EXECUTION_TASK_ERROR_CAUSE,
                        format!("{cause:?}"),
                    ));
                }
            }
            WorkerMessage::ProbePendingLocalStream {
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
            WorkerMessage::CreateLocalStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                replicas: _,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            WorkerMessage::CreateStorageStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                schema: _,
                context: _,
                result: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            WorkerMessage::FetchDriverStream {
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
            }
            WorkerMessage::FetchWorkerStream {
                owner,
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
                if let WorkerStreamOwner::Worker { worker_id, .. } = owner {
                    p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                }
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            WorkerMessage::FetchStorageStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                schema: _,
                context: _,
                result: _,
            } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                p.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            WorkerMessage::CleanUpJob { job_id, stage } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                if let Some(stage) = stage {
                    p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                }
            }
            WorkerMessage::Shutdown => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}

pub struct WorkerLocation {
    pub worker_id: WorkerId,
    pub host: String,
    pub port: u16,
}

impl From<WorkerLocation> for r#gen::WorkerLocation {
    fn from(value: WorkerLocation) -> Self {
        Self {
            worker_id: value.worker_id.into(),
            host: value.host,
            port: value.port as u32,
        }
    }
}

impl TryFrom<r#gen::WorkerLocation> for WorkerLocation {
    type Error = ExecutionError;

    fn try_from(value: r#gen::WorkerLocation) -> Result<Self, Self::Error> {
        let port = u16::try_from(value.port).map_err(|_| {
            ExecutionError::InvalidArgument(format!("invalid port: {}", value.port))
        })?;
        Ok(Self {
            worker_id: value.worker_id.into(),
            host: value.host,
            port,
        })
    }
}
