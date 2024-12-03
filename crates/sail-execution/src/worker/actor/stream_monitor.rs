use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::id::TaskId;
use crate::stream::RecordBatchStreamWriter;
use crate::worker::{WorkerActor, WorkerEvent};

pub(super) struct TaskStreamMonitor {
    handle: ActorHandle<WorkerActor>,
    task_id: TaskId,
    attempt: usize,
    stream: SendableRecordBatchStream,
    writer: Option<Box<dyn RecordBatchStreamWriter>>,
    signal: oneshot::Receiver<()>,
}

impl TaskStreamMonitor {
    pub fn new(
        handle: ActorHandle<WorkerActor>,
        task_id: TaskId,
        attempt: usize,
        stream: SendableRecordBatchStream,
        writer: Option<Box<dyn RecordBatchStreamWriter>>,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            task_id,
            attempt,
            stream,
            writer,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            task_id,
            attempt,
            stream,
            writer,
            signal,
        } = self;
        let event = Self::running(task_id, attempt);
        let _ = handle.send(event).await;
        let event = tokio::select! {
            x = Self::execute(task_id, attempt, stream, writer) => x,
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
        mut writer: Option<Box<dyn RecordBatchStreamWriter>>,
    ) -> WorkerEvent {
        let event = loop {
            let Some(batch) = stream.next().await else {
                break WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Succeeded,
                    message: None,
                };
            };
            match batch {
                Ok(batch) => {
                    if let Some(ref mut writer) = writer {
                        if let Err(e) = writer.write(batch).await {
                            break WorkerEvent::ReportTaskStatus {
                                task_id,
                                attempt,
                                status: TaskStatus::Failed,
                                message: Some(format!("failed to send batch: {e}")),
                            };
                        }
                    }
                }
                Err(e) => {
                    break WorkerEvent::ReportTaskStatus {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to read batch: {e}")),
                    }
                }
            }
        };
        if let Some(writer) = writer {
            if let Err(e) = writer.close() {
                return WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to close writer: {e}")),
                };
            }
        }
        event
    }
}
