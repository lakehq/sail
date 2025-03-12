use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::error::CommonErrorCause;
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
