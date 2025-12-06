use std::borrow::Cow;

use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::error::CommonErrorCause;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{TaskId, WorkerId};
use crate::stream::channel::ChannelName;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};

pub enum WorkerEvent {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    StartHeartbeat,
    RunTask {
        task_id: TaskId,
        attempt: usize,
        plan: Vec<u8>,
        partition: usize,
        channel: Option<ChannelName>,
    },
    StopTask {
        task_id: TaskId,
        attempt: usize,
    },
    ReportTaskStatus {
        task_id: TaskId,
        attempt: usize,
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
        host: String,
        port: u16,
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
            WorkerEvent::RunTask {
                task_id,
                attempt,
                plan: _,
                partition,
                channel,
            } => {
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
                p.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                if let Some(channel) = channel {
                    p.push((SpanAttribute::CLUSTER_CHANNEL_NAME, channel.to_string()));
                }
            }
            WorkerEvent::StopTask { task_id, attempt } => {
                p.push((SpanAttribute::CLUSTER_TASK_ID, task_id.to_string()));
                p.push((SpanAttribute::CLUSTER_TASK_ATTEMPT, attempt.to_string()));
            }
            WorkerEvent::ReportTaskStatus {
                task_id,
                attempt,
                status,
                message,
                cause,
            } => {
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
                host,
                port,
                channel,
                schema: _,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                p.push((SpanAttribute::CLUSTER_WORKER_HOST, host.clone()));
                p.push((SpanAttribute::CLUSTER_WORKER_PORT, port.to_string()));
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
