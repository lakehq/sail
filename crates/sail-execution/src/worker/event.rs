use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use tokio::sync::{mpsc, oneshot};

use crate::driver::state::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{TaskId, WorkerId};
use crate::stream::ChannelName;

pub enum WorkerEvent {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
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
    CreateMemoryTaskStream {
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<mpsc::Sender<RecordBatch>>>,
    },
    FetchThisWorkerTaskStream {
        channel: ChannelName,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    FetchOtherWorkerTaskStream {
        worker_id: WorkerId,
        host: String,
        port: u16,
        channel: ChannelName,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    Shutdown,
}
