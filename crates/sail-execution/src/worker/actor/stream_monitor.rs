use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::TaskInstance;
use crate::stream::error::TaskStreamError;
use crate::stream::writer::TaskStreamSink;
use crate::worker::{WorkerActor, WorkerEvent};

pub(super) struct TaskStreamMonitor {
    handle: ActorHandle<WorkerActor>,
    instance: TaskInstance,
    stream: SendableRecordBatchStream,
    sink: Option<Box<dyn TaskStreamSink>>,
    signal: oneshot::Receiver<()>,
}

impl TaskStreamMonitor {
    pub fn new(
        handle: ActorHandle<WorkerActor>,
        instance: TaskInstance,
        stream: SendableRecordBatchStream,
        sink: Option<Box<dyn TaskStreamSink>>,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            instance,
            stream,
            sink,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            instance,
            stream,
            sink,
            signal,
        } = self;
        let event = Self::running(instance.clone());
        let _ = handle.send(event).await;
        let event = tokio::select! {
            x = Self::execute(instance.clone(), stream, sink) => x,
            x = Self::cancel(instance.clone(), signal) => x,
        };
        let _ = handle.send(event).await;
    }

    fn running(instance: TaskInstance) -> WorkerEvent {
        WorkerEvent::ReportTaskStatus {
            instance,
            status: TaskStatus::Running,
            message: None,
            cause: None,
        }
    }

    async fn cancel(instance: TaskInstance, signal: oneshot::Receiver<()>) -> WorkerEvent {
        let _ = signal.await;
        WorkerEvent::ReportTaskStatus {
            instance: instance.clone(),
            status: TaskStatus::Canceled,
            message: Some(format!(
                "job {} task {} attempt {} canceled",
                instance.job_id, instance.task_id, instance.attempt
            )),
            cause: None,
        }
    }

    async fn execute(
        instance: TaskInstance,
        mut stream: SendableRecordBatchStream,
        mut sink: Option<Box<dyn TaskStreamSink>>,
    ) -> WorkerEvent {
        let event = loop {
            let Some(batch) = stream.next().await else {
                break WorkerEvent::ReportTaskStatus {
                    instance: instance.clone(),
                    status: TaskStatus::Succeeded,
                    message: None,
                    cause: None,
                };
            };
            let error = match &batch {
                Ok(_) => None,
                Err(e) => Some((
                    format!("failed to read batch: {e}"),
                    CommonErrorCause::new::<PyErrExtractor>(e),
                )),
            };
            if let Some(ref mut sink) = sink {
                if let Err(e) = sink
                    .write(batch.map_err(|e| TaskStreamError::External(Arc::new(e))))
                    .await
                {
                    break WorkerEvent::ReportTaskStatus {
                        instance: instance.clone(),
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to send batch: {e}")),
                        cause: None,
                    };
                }
            }
            if let Some((message, cause)) = error {
                break WorkerEvent::ReportTaskStatus {
                    instance: instance.clone(),
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                };
            }
        };
        if let Some(sink) = sink {
            if let Err(e) = sink.close() {
                return WorkerEvent::ReportTaskStatus {
                    instance: instance.clone(),
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to close writer: {e}")),
                    cause: None,
                };
            }
        }
        event
    }
}
