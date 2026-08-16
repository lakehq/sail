use std::panic::AssertUnwindSafe;

use datafusion::execution::SendableRecordBatchStream;
use futures::{FutureExt, StreamExt};
use sail_common::actor::ActorHandle;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage, panic_message};

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
        let message = Self::monitor(key, stream, signal).await;
        let _ = handle.send(message).await;
    }

    async fn monitor(
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
    ) -> TaskRunnerMessage {
        let panic_key = key.clone();
        let result = AssertUnwindSafe(async move {
            tokio::select! {
                x = Self::execute(key.clone(), stream) => x,
                x = Self::cancel(key, signal) => x,
            }
        })
        .catch_unwind()
        .await;
        match result {
            Ok(message) => message,
            Err(payload) => {
                let message = format!("task panicked: {}", panic_message(payload));
                Self::status(
                    panic_key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                )
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
    use std::sync::Arc;
    use std::task::Poll;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    use super::*;
    use crate::id::JobId;

    #[tokio::test]
    async fn reports_stream_panic_as_task_failure() -> std::result::Result<(), String> {
        let key = TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
        };
        let stream = stream::poll_fn(|_| -> Poll<Option<Result<RecordBatch>>> {
            std::panic::resume_unwind(Box::new("test stream panic"))
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        ));
        let (_signal, receiver) = oneshot::channel();

        let message = TaskMonitor::monitor(key.clone(), stream, receiver).await;

        let TaskRunnerMessage::ReportTaskStatus {
            key: actual_key,
            status: TaskStatus::Failed,
            message: Some(message),
            cause: Some(CommonErrorCause::Internal(cause)),
        } = message
        else {
            return Err("expected a failed task status".to_string());
        };
        assert_eq!(actual_key, key);
        assert_eq!(message, "task panicked: test stream panic");
        assert_eq!(cause, message);
        Ok(())
    }

    #[tokio::test]
    async fn reports_task_cancellation() -> std::result::Result<(), String> {
        let key = TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
        };
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::pending(),
        ));
        let (signal, receiver) = oneshot::channel();
        assert!(signal.send(()).is_ok());

        let message = TaskMonitor::monitor(key.clone(), stream, receiver).await;

        let TaskRunnerMessage::ReportTaskStatus {
            key: actual_key,
            status: TaskStatus::Canceled,
            message: Some(message),
            cause: None,
        } = message
        else {
            return Err("expected a canceled task status".to_string());
        };
        assert_eq!(actual_key, key);
        assert_eq!(message, "job 1 stage 2 partition 3 attempt 4 canceled");
        Ok(())
    }
}
