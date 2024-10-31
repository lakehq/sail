use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_server::actor::ActorHandle;
use tokio::sync::{mpsc, oneshot};

use crate::driver::state::TaskStatus;
use crate::id::TaskId;
use crate::worker::{WorkerActor, WorkerEvent};

pub(super) struct TaskStreamMonitor {
    handle: ActorHandle<WorkerActor>,
    task_id: TaskId,
    attempt: usize,
    stream: SendableRecordBatchStream,
    sender: Option<mpsc::Sender<Result<RecordBatch>>>,
    signal: oneshot::Receiver<()>,
}

impl TaskStreamMonitor {
    pub fn new(
        handle: ActorHandle<WorkerActor>,
        task_id: TaskId,
        attempt: usize,
        stream: SendableRecordBatchStream,
        sender: Option<mpsc::Sender<Result<RecordBatch>>>,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            task_id,
            attempt,
            stream,
            sender,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            task_id,
            attempt,
            stream,
            sender,
            signal,
        } = self;
        let event = Self::running(task_id, attempt);
        let _ = handle.send(event).await;
        let event = tokio::select! {
            x = Self::execute(task_id, attempt, stream, sender) => x,
            x = Self::cancel(task_id, attempt, signal) => x,
        };
        let _ = handle.send(event).await;
    }

    fn running(task_id: TaskId, attempt: usize) -> WorkerEvent {
        WorkerEvent::ReportTaskStatus {
            task_id,
            attempt,
            status: TaskStatus::Running,
            message: None,
        }
    }

    async fn cancel(task_id: TaskId, attempt: usize, signal: oneshot::Receiver<()>) -> WorkerEvent {
        let _ = signal.await;
        WorkerEvent::ReportTaskStatus {
            task_id,
            attempt,
            status: TaskStatus::Canceled,
            message: Some(format!("task {task_id} attempt {attempt} canceled")),
        }
    }

    async fn execute(
        task_id: TaskId,
        attempt: usize,
        mut stream: SendableRecordBatchStream,
        sender: Option<mpsc::Sender<Result<RecordBatch>>>,
    ) -> WorkerEvent {
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    if let Some(ref sender) = sender {
                        if let Err(e) = sender.send(Ok(batch)).await {
                            return WorkerEvent::ReportTaskStatus {
                                task_id,
                                attempt,
                                status: TaskStatus::Failed,
                                message: Some(format!("failed to send batch: {e}")),
                            };
                        }
                    }
                }
                Err(e) => {
                    return WorkerEvent::ReportTaskStatus {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to read batch: {e}")),
                    }
                }
            }
        }
        WorkerEvent::ReportTaskStatus {
            task_id,
            attempt,
            status: TaskStatus::Succeeded,
            message: None,
        }
    }
}
