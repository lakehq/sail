use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::worker::{WorkerActor, WorkerEvent};

pub(super) struct TaskMonitor {
    handle: ActorHandle<WorkerActor>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl TaskMonitor {
    pub fn new(
        handle: ActorHandle<WorkerActor>,
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            handle,
            key,
            stream,
            signal,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            key,
            stream,
            signal,
        } = self;
        let event = Self::running(key.clone());
        let _ = handle.send(event).await;
        let event = tokio::select! {
            x = Self::execute(key.clone(), stream) => x,
            x = Self::cancel(key.clone(), signal) => x,
        };
        let _ = handle.send(event).await;
    }

    fn running(key: TaskKey) -> WorkerEvent {
        WorkerEvent::ReportTaskStatus {
            key,
            status: TaskStatus::Running,
            message: None,
            cause: None,
        }
    }

    async fn cancel(key: TaskKey, signal: oneshot::Receiver<()>) -> WorkerEvent {
        let _ = signal.await;
        WorkerEvent::ReportTaskStatus {
            key: key.clone(),
            status: TaskStatus::Canceled,
            message: Some(format!("{} canceled", TaskKeyDisplay(&key))),
            cause: None,
        }
    }

    async fn execute(key: TaskKey, mut stream: SendableRecordBatchStream) -> WorkerEvent {
        let event = loop {
            let Some(batch) = stream.next().await else {
                break WorkerEvent::ReportTaskStatus {
                    key: key.clone(),
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
            if let Some((message, cause)) = error {
                break WorkerEvent::ReportTaskStatus {
                    key: key.clone(),
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                };
            }
        };
        event
    }
}
