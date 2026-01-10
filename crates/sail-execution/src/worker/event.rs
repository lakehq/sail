use std::borrow::Cow;

use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::error::CommonErrorCause;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::task::definition::TaskDefinition;
use crate::worker::gen;

pub enum WorkerEvent {
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
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchWorkerStream {
        owner: WorkerStreamOwner,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchRemoteStream {
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
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

impl SpanAssociation for WorkerEvent {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            WorkerEvent::ServerReady { .. } => "ServerReady",
            WorkerEvent::StartHeartbeat => "StartHeartbeat",
            WorkerEvent::ReportKnownPeers { .. } => "ReportKnownPeers",
            WorkerEvent::RunTask { .. } => "RunTask",
            WorkerEvent::StopTask { .. } => "StopTask",
            WorkerEvent::ReportTaskStatus { .. } => "ReportTaskStatus",
            WorkerEvent::ProbePendingLocalStream { .. } => "ProbePendingLocalStream",
            WorkerEvent::CreateLocalStream { .. } => "CreateLocalStream",
            WorkerEvent::CreateRemoteStream { .. } => "CreateRemoteStream",
            WorkerEvent::FetchDriverStream { .. } => "FetchDriverStream",
            WorkerEvent::FetchWorkerStream { .. } => "FetchWorkerStream",
            WorkerEvent::FetchRemoteStream { .. } => "FetchRemoteStream",
            WorkerEvent::CleanUpJob { .. } => "CleanUpJob",
            WorkerEvent::Shutdown => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            WorkerEvent::ServerReady { port, signal: _ } => {
                p.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
            }
            WorkerEvent::StartHeartbeat => {}
            WorkerEvent::ReportKnownPeers { peer_worker_ids: _ } => {}
            WorkerEvent::RunTask {
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
            WorkerEvent::StopTask {
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
            WorkerEvent::ReportTaskStatus {
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
            WorkerEvent::ProbePendingLocalStream {
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
            WorkerEvent::CreateLocalStream {
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
            WorkerEvent::CreateRemoteStream {
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
            WorkerEvent::FetchDriverStream {
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
            WorkerEvent::FetchWorkerStream {
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
            WorkerEvent::FetchRemoteStream {
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
            WorkerEvent::CleanUpJob { job_id, stage } => {
                p.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                if let Some(stage) = stage {
                    p.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                }
            }
            WorkerEvent::Shutdown => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}

pub struct WorkerLocation {
    pub worker_id: WorkerId,
    pub host: String,
    pub port: u16,
}

impl From<WorkerLocation> for gen::WorkerLocation {
    fn from(value: WorkerLocation) -> Self {
        Self {
            worker_id: value.worker_id.into(),
            host: value.host,
            port: value.port as u32,
        }
    }
}

impl TryFrom<gen::WorkerLocation> for WorkerLocation {
    type Error = ExecutionError;

    fn try_from(value: gen::WorkerLocation) -> Result<Self, Self::Error> {
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
