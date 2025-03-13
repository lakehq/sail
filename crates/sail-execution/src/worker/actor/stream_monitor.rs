use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::driver::state::TaskStatus;
use crate::id::TaskId;
use crate::stream::error::TaskStreamError;
use crate::stream::writer::TaskStreamSink;
use crate::worker::{WorkerActor, WorkerEvent};

pub(super) struct TaskStreamMonitor {
    handle: ActorHandle<WorkerActor>,
    task_id: TaskId,
    attempt: usize,
    stream: SendableRecordBatchStream,
    sink: Option<Box<dyn TaskStreamSink>>,
    signal: oneshot::Receiver<()>,
}

impl TaskStreamMonitor {
    pub fn new(
        handle: ActorHandle<WorkerActor>,
        task_id: TaskId,
        attempt: usize,
        stream: SendableRecordBatchStream,
        sink: Option<Box<dyn TaskStreamSink>>,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            task_id,
            attempt,
            stream,
            sink,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            task_id,
            attempt,
            stream,
            sink,
            signal,
        } = self;
        let event = Self::running(task_id, attempt);
        let _ = handle.send(event).await;
        let event = tokio::select! {
            x = Self::execute(task_id, attempt, stream, sink) => x,
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
            cause: None,
        }
    }

    async fn cancel(task_id: TaskId, attempt: usize, signal: oneshot::Receiver<()>) -> WorkerEvent {
        let _ = signal.await;
        WorkerEvent::ReportTaskStatus {
            task_id,
            attempt,
            status: TaskStatus::Canceled,
            message: Some(format!("task {task_id} attempt {attempt} canceled")),
            cause: None,
        }
    }

    async fn execute(
        task_id: TaskId,
        attempt: usize,
        mut stream: SendableRecordBatchStream,
        mut sink: Option<Box<dyn TaskStreamSink>>,
    ) -> WorkerEvent {
        let event = loop {
            let Some(batch) = stream.next().await else {
                break WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Succeeded,
                    message: None,
                    cause: None,
                };
            };
            let error = match &batch {
                Ok(_) => None,
                Err(e) => Some((
                    format!("failed to read batch: {e}"),
                    CommonErrorCause::new(e),
                )),
            };
            if let Some(ref mut sink) = sink {
                if let Err(e) = sink
                    .write(batch.map_err(|e| TaskStreamError::External(Arc::new(e))))
                    .await
                {
                    break WorkerEvent::ReportTaskStatus {
                        task_id,
                        attempt,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to send batch: {e}")),
                        cause: None,
                    };
                }
            }
            if let Some((message, cause)) = error {
                break WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                };
            }
        };
        if let Some(sink) = sink {
            if let Err(e) = sink.close() {
                return WorkerEvent::ReportTaskStatus {
                    task_id,
                    attempt,
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to close writer: {e}")),
                    cause: None,
                };
            }
        }
        event
    }
}
