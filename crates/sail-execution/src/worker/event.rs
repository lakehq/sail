use std::borrow::Cow;

use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::error::CommonErrorCause;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskInstance, WorkerId};
use crate::stream::channel::ChannelName;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
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
        instance: TaskInstance,
        plan: Vec<u8>,
        partition: usize,
        channel: Option<ChannelName>,
        peers: Vec<WorkerLocation>,
    },
    StopTask {
        instance: TaskInstance,
    },
    ReportTaskStatus {
        instance: TaskInstance,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    },
    CreateLocalStream {
        channel: ChannelName,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    },
    CreateRemoteStream {
        uri: String,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    },
    FetchThisWorkerStream {
        channel: ChannelName,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchOtherWorkerStream {
        worker_id: WorkerId,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchRemoteStream {
        uri: String,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    RemoveLocalStream {
        channel_prefix: String,
    },
    Shutdown,
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
            WorkerEvent::CreateLocalStream { .. } => "CreateLocalStream",
            WorkerEvent::CreateRemoteStream { .. } => "CreateRemoteStream",
            WorkerEvent::FetchThisWorkerStream { .. } => "FetchThisWorkerStream",
            WorkerEvent::FetchOtherWorkerStream { .. } => "FetchOtherWorkerStream",
            WorkerEvent::FetchRemoteStream { .. } => "FetchRemoteStream",
            WorkerEvent::RemoveLocalStream { .. } => "RemoveLocalStream",
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
                instance:
                    TaskInstance {
                        job_id,
                        task_id,
                        attempt,
                    },
                plan: _,
                partition,
                channel,
                peers: _,
            } => {
                p.push((SpanAttribute::CLUSTER_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                if let Some(channel) = channel {
                    p.push((SpanAttribute::CLUSTER_CHANNEL_NAME, channel.to_string()));
                }
            }
            WorkerEvent::StopTask {
                instance:
                    TaskInstance {
                        job_id,
                        task_id,
                        attempt,
                    },
            } => {
                p.push((SpanAttribute::CLUSTER_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
            }
            WorkerEvent::ReportTaskStatus {
                instance:
                    TaskInstance {
                        job_id,
                        task_id,
                        attempt,
                    },
                status,
                message,
                cause,
            } => {
                p.push((SpanAttribute::CLUSTER_JOB_ID, job_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_STATUS, status.to_string()));
                if let Some(msg) = message {
                    p.push((SpanAttribute::CLUSTER_TASK_MESSAGE, msg.clone()));
                }
                if let Some(cause) = cause {
                    p.push((
                        SpanAttribute::CLUSTER_TASK_ERROR_CAUSE,
                        format!("{cause:?}"),
                    ));
                }
            }
            WorkerEvent::CreateLocalStream {
                channel,
                storage,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_CHANNEL_NAME, channel.to_string()));
                p.push((
                    SpanAttribute::CLUSTER_STREAM_LOCAL_STORAGE,
                    storage.to_string(),
                ));
            }
            WorkerEvent::CreateRemoteStream {
                uri,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_STREAM_REMOTE_URI, uri.clone()));
            }
            WorkerEvent::FetchThisWorkerStream { channel, result: _ } => {
                p.push((SpanAttribute::CLUSTER_CHANNEL_NAME, channel.to_string()));
            }
            WorkerEvent::FetchOtherWorkerStream {
                worker_id,
                channel,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                p.push((SpanAttribute::CLUSTER_CHANNEL_NAME, channel.to_string()));
            }
            WorkerEvent::FetchRemoteStream {
                uri,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_STREAM_REMOTE_URI, uri.clone()));
            }
            WorkerEvent::RemoveLocalStream { channel_prefix } => {
                p.push((
                    SpanAttribute::CLUSTER_CHANNEL_PREFIX,
                    channel_prefix.clone(),
                ));
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
