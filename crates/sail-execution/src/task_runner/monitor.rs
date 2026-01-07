use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorHandle};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::task_runner::TaskRunnerMessage;

pub struct TaskMonitor<T: Actor> {
    handle: ActorHandle<T>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl<T: Actor> TaskMonitor<T> {
    pub fn new(
        handle: ActorHandle<T>,
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
}

impl<T: Actor> TaskMonitor<T>
where
    T::Message: TaskRunnerMessage,
{
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

    fn running(key: TaskKey) -> T::Message {
        T::Message::report_task_status(key, TaskStatus::Running, None, None)
    }

    async fn cancel(key: TaskKey, signal: oneshot::Receiver<()>) -> T::Message {
        let _ = signal.await;
        T::Message::report_task_status(
            key.clone(),
            TaskStatus::Canceled,
            Some(format!("{} canceled", TaskKeyDisplay(&key))),
            None,
        )
    }

    async fn execute(key: TaskKey, mut stream: SendableRecordBatchStream) -> T::Message {
        let event = loop {
            let Some(batch) = stream.next().await else {
                break T::Message::report_task_status(
                    key.clone(),
                    TaskStatus::Succeeded,
                    None,
                    None,
                );
            };
            let error = match &batch {
                Ok(_) => None,
                Err(e) => Some((
                    format!("task error: {e}"),
                    CommonErrorCause::new::<PyErrExtractor>(e),
                )),
            };
            if let Some((message, cause)) = error {
                break T::Message::report_task_status(
                    key.clone(),
                    TaskStatus::Failed,
                    Some(message),
                    Some(cause),
                );
            }
        };
        event
    }
}
