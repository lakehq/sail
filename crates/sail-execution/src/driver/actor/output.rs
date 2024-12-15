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
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    Running {
        signal: oneshot::Sender<String>,
    },
}

impl JobOutput {
    pub fn run(
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
        stream: SendableRecordBatchStream,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) -> Self {
        let (tx, rx) = oneshot::channel();
        let handle = ctx.handle().clone();
        ctx.spawn(Self::output(handle, job_id, stream, rx, sender));
        JobOutput::Running { signal: tx }
    }

    async fn output(
        handle: ActorHandle<DriverActor>,
        job_id: JobId,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<String>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        tokio::select! {
            _ = Self::read(stream, sender.clone()) => {},
            _ = Self::stop(signal, sender) => {},
        }
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

    async fn stop(signal: oneshot::Receiver<String>, sender: mpsc::Sender<Result<RecordBatch>>) {
        if let Ok(reason) = signal.await {
            if let Err(e) = sender.send(exec_err!("{reason}")).await {
                error!("failed to send job output stop signal: {e}");
            }
        }
    }

    pub fn fail(self, reason: String) {
        match self {
            JobOutput::Pending { result } => {
                let _ = result.send(Err(ExecutionError::InternalError(reason)));
            }
            JobOutput::Running { signal } => {
                let _ = signal.send(reason);
            }
        }
    }
}
