use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{TaskId, WorkerId};
use crate::stream::{ChannelName, LocalStreamStorage, RecordBatchStreamWriter};

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
    },
    CreateLocalStream {
        channel: ChannelName,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn RecordBatchStreamWriter>>>,
    },
    CreateRemoteStream {
        uri: String,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn RecordBatchStreamWriter>>>,
    },
    FetchThisWorkerStream {
        channel: ChannelName,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    FetchOtherWorkerStream {
        worker_id: WorkerId,
        host: String,
        port: u16,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    FetchRemoteStream {
        uri: String,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    RemoveLocalStream {
        channel_prefix: String,
    },
    Shutdown,
}
