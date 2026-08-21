use std::any::Any;

use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common::actor::ActorHandle;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;
use tokio_util::task::AbortOnDropHandle;

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

    pub async fn supervise(self) {
        let handle = self.handle.clone();
        let key = self.key.clone();
        let monitor = AbortOnDropHandle::new(tokio::spawn(self.run()));
        if let Some(message) = Self::monitor_failure(key, monitor).await {
            let _ = handle.send(message).await;
        }
    }

    async fn monitor_failure(
        key: TaskKey,
        monitor: AbortOnDropHandle<()>,
    ) -> Option<TaskRunnerMessage> {
        match monitor.await {
            Ok(()) => None,
            Err(error) if error.is_cancelled() => None,
            Err(error) if error.is_panic() => {
                let message = format!("task panicked: {}", Self::panic_message(error.into_panic()));
                Some(Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                ))
            }
            Err(error) => {
                let message = format!("task monitor failed: {error}");
                Some(Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                ))
            }
        }
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

    fn panic_message(payload: Box<dyn Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic".to_string()
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

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;
    use crate::id::JobId;

    fn task_key() -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
        }
    }

    #[tokio::test]
    async fn ignores_successful_monitor_completion() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {}));

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn ignores_cancelled_monitor() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(future::pending()));
        monitor.abort();

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn reports_monitor_panic_as_task_failure() -> Result<(), String> {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {
            std::panic::resume_unwind(Box::new("test monitor panic"));
        }));

        let message = TaskMonitor::monitor_failure(task_key(), monitor)
            .await
            .ok_or_else(|| "expected a failed task status".to_string())?;

        let TaskRunnerMessage::ReportTaskStatus {
            key,
            status: TaskStatus::Failed,
            message: Some(message),
            cause: Some(CommonErrorCause::Internal(cause)),
        } = message
        else {
            return Err("expected a failed task status".to_string());
        };
        assert_eq!(key, task_key());
        assert_eq!(message, "task panicked: test monitor panic");
        assert_eq!(cause, message);
        Ok(())
    }

    #[test]
    fn extracts_panic_messages() {
        assert_eq!(
            TaskMonitor::panic_message(Box::new("literal panic")),
            "literal panic"
        );
        assert_eq!(
            TaskMonitor::panic_message(Box::new("owned panic".to_string())),
            "owned panic"
        );
        assert_eq!(TaskMonitor::panic_message(Box::new(42)), "unknown panic");
    }
}
