use datafusion::arrow::array::RecordBatch;
use datafusion::common::{exec_err, Result};
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use log::error;
use sail_server::actor::{ActorContext, ActorHandle};
use tokio::sync::{mpsc, oneshot};

use crate::driver::{DriverActor, DriverEvent};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::JobId;

pub(super) enum JobOutput {
    Pending {
        /// The sender for the signal of job completion,
        /// or [`None`] if the signal has already been sent.
        tx: Option<oneshot::Sender<Option<String>>>,
        /// The receiver for the signal of job completion.
        rx: oneshot::Receiver<Option<String>>,
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    Running {
        /// The sender for the signal of job completion,
        /// or [`None`] if the signal has already been sent.
        tx: Option<oneshot::Sender<Option<String>>>,
    },
}

impl JobOutput {
    pub fn run(
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
        stream: SendableRecordBatchStream,
        sender: mpsc::Sender<Result<RecordBatch>>,
        tx: Option<oneshot::Sender<Option<String>>>,
        rx: oneshot::Receiver<Option<String>>,
    ) -> Self {
        let handle = ctx.handle().clone();
        ctx.spawn(Self::output(handle, job_id, stream, rx, sender));
        JobOutput::Running { tx }
    }

    async fn output(
        handle: ActorHandle<DriverActor>,
        job_id: JobId,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<Option<String>>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        Self::read(stream, sender.clone()).await;
        Self::wait(signal, sender).await;
        if let Err(e) = handle.send(DriverEvent::RemoveJobOutput { job_id }).await {
            error!("failed to remove job output: {e}");
        }
    }

    async fn read(
        mut stream: SendableRecordBatchStream,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        while let Some(batch) = stream.next().await {
            if let Err(e) = sender.send(batch).await {
                error!("failed to send job output record batch: {e}");
                break;
            }
        }
    }

    async fn wait(
        signal: oneshot::Receiver<Option<String>>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        if let Ok(Some(reason)) = signal.await {
            if let Err(e) = sender.send(exec_err!("{reason}")).await {
                error!("failed to send job output stop signal: {e}");
            }
        }
    }

    pub fn fail(self, reason: String) {
        match self {
            JobOutput::Pending {
                result,
                tx: _,
                rx: _,
            } => {
                let _ = result.send(Err(ExecutionError::InternalError(reason)));
                // There is no need to send the failure to `tx` since `rx` is being dropped.
            }
            JobOutput::Running { tx } => {
                if let Some(tx) = tx {
                    let _ = tx.send(Some(reason));
                }
            }
        }
    }

    pub fn succeed(self) -> Option<Self> {
        match self {
            JobOutput::Pending { result, tx, rx } => {
                if let Some(tx) = tx {
                    let _ = tx.send(None);
                }
                Some(JobOutput::Pending {
                    result,
                    tx: None,
                    rx,
                })
            }
            JobOutput::Running { tx } => {
                if let Some(tx) = tx {
                    let _ = tx.send(None);
                }
                None
            }
        }
    }
}
