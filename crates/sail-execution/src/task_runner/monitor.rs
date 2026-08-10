use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common::actor::ActorHandle;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

pub struct TaskMonitor {
    handle: ActorHandle<TaskRunnerActor>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl TaskMonitor {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
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
        let _ = handle.send(Self::running(key.clone())).await;
        let message = tokio::select! {
            x = Self::execute(key.clone(), stream) => x,
            x = Self::cancel(key.clone(), signal) => x,
        };
        let _ = handle.send(message).await;
    }

    /// Builds a "task is running" status message.
    fn running(key: TaskKey) -> TaskRunnerMessage {
        Self::status(key, TaskStatus::Running, None, None)
    }

    fn status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> TaskRunnerMessage {
        TaskRunnerMessage::ReportTaskStatus {
            key,
            status,
            message,
            cause,
        }
    }

    /// Waits for a cancellation signal and builds a canceled status message.
    async fn cancel(key: TaskKey, signal: oneshot::Receiver<()>) -> TaskRunnerMessage {
        let _ = signal.await;
        Self::status(
            key.clone(),
            TaskStatus::Canceled,
            Some(format!("{} canceled", TaskKeyDisplay(&key))),
            None,
        )
    }

    async fn execute(key: TaskKey, mut stream: SendableRecordBatchStream) -> TaskRunnerMessage {
        loop {
            let Some(batch) = stream.next().await else {
                break Self::status(key, TaskStatus::Succeeded, None, None);
            };
            if let Err(error) = batch {
                break Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(format!("task error: {error}")),
                    Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                );
            }
        }
    }
}
