use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::StreamExt;
use sail_common::actor::ActorHandle;
use sail_common::telemetry::SpanAttribute;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_system_store::types::TaskOutputStatistics;
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
    output_statistics: Arc<OnceLock<TaskOutputStatistics>>,
}

impl TaskMonitor {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        output_statistics: Arc<OnceLock<TaskOutputStatistics>>,
    ) -> Self {
        Self {
            handle,
            key,
            stream,
            signal,
            output_statistics,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            key,
            stream,
            signal,
            output_statistics,
        } = self;
        let _ = handle.send(Self::running(key.clone())).await;
        let message = tokio::select! {
            x = Self::execute(key.clone(), stream, output_statistics) => x,
            x = Self::cancel(key.clone(), signal) => x,
        };
        let _ = handle.send(message).await;
    }

    pub async fn supervise(self) {
        let handle = self.handle.clone();
        let key = self.key.clone();
        // Admission now has one span per batch. Preserve task identity on the monitor
        // so preparation and execution spans can still be attributed to each attempt.
        let span = Span::enter_with_local_parent("TaskMonitor::run").with_properties(|| {
            [
                (SpanAttribute::EXECUTION_JOB_ID, key.job_id.to_string()),
                (SpanAttribute::EXECUTION_STAGE, key.stage.to_string()),
                (
                    SpanAttribute::EXECUTION_PARTITION,
                    key.partition.to_string(),
                ),
                (SpanAttribute::EXECUTION_ATTEMPT, key.attempt.to_string()),
            ]
        });
        let monitor = AbortOnDropHandle::new(tokio::spawn(self.run().in_span(span)));
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
            output_statistics: None,
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

    async fn execute(
        key: TaskKey,
        mut stream: SendableRecordBatchStream,
        output_statistics: Arc<OnceLock<TaskOutputStatistics>>,
    ) -> TaskRunnerMessage {
        loop {
            let Some(batch) = stream.next().await else {
                break TaskRunnerMessage::ReportTaskStatus {
                    key,
                    status: TaskStatus::Succeeded,
                    message: None,
                    cause: None,
                    output_statistics: output_statistics.get().cloned(),
                };
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

    #[tokio::test]
    async fn only_successful_completion_carries_statistics() {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::common::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let statistics = Arc::new(OnceLock::from(TaskOutputStatistics {
            complete: false,
            partitions: vec![],
        }));
        let empty = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::empty(),
        ));
        let message = TaskMonitor::execute(task_key(), empty, statistics.clone()).await;
        assert!(matches!(
            message,
            TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Succeeded,
                output_statistics: Some(TaskOutputStatistics {
                    complete: false,
                    ..
                }),
                ..
            }
        ));
        let failed = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::iter(vec![Err(DataFusionError::Execution("failed".into()))]),
        ));
        let message = TaskMonitor::execute(task_key(), failed, statistics).await;
        assert!(matches!(
            message,
            TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Failed,
                output_statistics: None,
                ..
            }
        ));
        let (tx, rx) = oneshot::channel();
        drop(tx);
        assert!(matches!(
            TaskMonitor::cancel(task_key(), rx).await,
            TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Canceled,
                output_statistics: None,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn preserves_unknown_commit_cause_through_task_status_and_remote_error()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::common::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use sail_catalog::error::CatalogError;
        use sail_common_datafusion::error::RemoteError;

        let error = DataFusionError::Collection(vec![
            DataFusionError::Execution("another failure".into()),
            DataFusionError::Context(
                "commit failed".into(),
                Box::new(DataFusionError::External(Box::new(
                    CatalogError::CommitStateUnknown("response lost".to_string().into()),
                ))),
            ),
        ]);
        let stream = RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::iter([Err(error)]),
        );
        let message =
            TaskMonitor::execute(task_key(), Box::pin(stream), Arc::new(OnceLock::new())).await;
        let TaskRunnerMessage::ReportTaskStatus {
            status: TaskStatus::Failed,
            cause: Some(cause),
            ..
        } = message
        else {
            return Err("expected task failure with cause".into());
        };
        assert!(!cause.is_retryable());
        let cause = serde_json::from_str(&serde_json::to_string(&cause)?)?;
        let error = DataFusionError::External(Box::new(RemoteError { cause }));
        assert!(matches!(
            CommonErrorCause::new::<PyErrExtractor>(&error),
            CommonErrorCause::CommitStateUnknown(message) if message == "commit state unknown: response lost"
        ));
        Ok(())
    }

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
            output_statistics: None,
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
