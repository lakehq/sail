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

pub enum JobOutput {
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
        // If the task fails, the consumer of the job output will receive an error
        // from either the stream ("data plane") or the stop signal ("control plane").
        // We cannot guarantee which error will be received. Fortunately, the error
        // will appear to be the same to the consumer, since they are standardized
        // via `CommonErrorCause`.
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
            let error = DataFusionError::External(Box::new(TaskStreamError::from(cause)));
            if let Err(e) = sender.send(Err(error)).await {
                error!("failed to send job output stop signal: {e}");
            }
        }
    }

    pub fn fail(self, ctx: &mut ActorContext<DriverActor>, job_id: JobId, cause: CommonErrorCause) {
        match self {
            JobOutput::Pending { result } => {
                // The job output can be pending when this function is called.
                // This happens when the task "running" and "failed" events are received
                // out of order. In this case, the stale "running" event is ignored,
                // and the job output never transitions to the running state.
                // So we create a data stream here and send the error to the job output consumer.
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
            }
        }
    }
}
