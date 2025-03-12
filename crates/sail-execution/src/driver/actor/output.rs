use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use log::error;
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::{ActorContext, ActorHandle};
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::{DriverActor, DriverEvent};
use crate::error::ExecutionResult;
use crate::id::JobId;
use crate::stream::error::TaskStreamError;

/// The time to wait after the stop signal is received.
///
/// In [`JobOutput::output`], we use `tokio::select!` to wait for either the task that reads
/// the stream or the task that waits for the stop signal.
/// A grace period after receiving the stop signal allows the job output to return error from the
/// stream first, before the failure in the stop signal get a chance to be returned as an error.
/// This can provider better error message to the consumer of the job output.
///
/// When the stop signal contains a failure, the stream will usually be closed immediately by the
/// remote task, so the job output does not have to wait the entire grace period before completing.
const STOP_SIGNAL_GRACE_PERIOD: tokio::time::Duration = tokio::time::Duration::from_secs(10);

pub(super) enum JobOutput {
    Pending {
        result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    },
    Running {
        signal: oneshot::Sender<CommonErrorCause>,
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
        signal: oneshot::Receiver<CommonErrorCause>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        tokio::select! {
            _ = Self::read(stream, sender.clone()) => {},
            _ = Self::stop(signal, sender) => {},
        }
        Self::finalize(handle, job_id).await;
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

    async fn finalize(handle: ActorHandle<DriverActor>, job_id: JobId) {
        if let Err(e) = handle.send(DriverEvent::CleanUpJob { job_id }).await {
            error!("failed to clean up job: {e}");
        }
    }

    async fn stop(
        signal: oneshot::Receiver<CommonErrorCause>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        if let Ok(cause) = signal.await {
            // If the stream produces an error in `Self::read()` during the grace period, the error
            // will be sent to the job output, and the read task will complete first. In this case,
            // `tokio::select!` will terminate `Self::stop()` as well, and the error from the stop
            // signal will not be sent to the job output.
            tokio::time::sleep(STOP_SIGNAL_GRACE_PERIOD).await;
            let error = DataFusionError::External(Box::new(TaskStreamError::from(cause)));
            if let Err(e) = sender.send(Err(error)).await {
                error!("failed to send job output stop signal: {e}");
            }
        }
    }

    pub fn fail(self, ctx: &mut ActorContext<DriverActor>, job_id: JobId, cause: CommonErrorCause) {
        match self {
            JobOutput::Pending { result } => {
                let (tx, rx) = mpsc::channel(1);
                let stream = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::new(Schema::empty()),
                    ReceiverStream::new(rx),
                ));
                let handle = ctx.handle().clone();
                ctx.spawn(async move {
                    let _ = tx
                        .send(Err(DataFusionError::External(Box::new(
                            TaskStreamError::from(cause),
                        ))))
                        .await;
                    let _ = result.send(Ok(stream));
                    Self::finalize(handle, job_id).await;
                });
            }
            JobOutput::Running { signal } => {
                let _ = signal.send(cause);
                // `Self::finalize()` will be called in the spawned task after receiving the signal.
            }
        }
    }
}
