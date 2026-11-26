// SPDX-License-Identifier: Apache-2.0

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use datafusion::common::{Result, exec_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{Stream, stream};
use tokio_util::sync::CancellationToken;

use crate::extension::SessionExtension;

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    /// Executes a plan.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    /// Returns the distributed execution plan for EXPLAIN output.
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String>;

    async fn stop(&self);
}

pub struct JobService {
    runner: Box<dyn JobRunner>,
}

impl JobService {
    pub fn new(runner: Box<dyn JobRunner>) -> Self {
        Self { runner }
    }

    pub fn runner(&self) -> &dyn JobRunner {
        self.runner.as_ref()
    }
}

impl SessionExtension for JobService {
    fn name() -> &'static str {
        "JobService"
    }
}

/// Cancels query consumers when their server or driver shuts down.
#[derive(Default)]
pub struct QueryCancellation {
    signal: CancellationToken,
}

impl QueryCancellation {
    pub fn cancel(&self) {
        self.signal.cancel();
    }

    pub async fn cancelled(&self) {
        self.signal.cancelled().await;
    }

    fn from_context(ctx: &SessionContext) -> Option<Arc<Self>> {
        ctx.state_ref().read().config().get_extension::<Self>()
    }

    pub fn cancel_stream_on_shutdown(
        ctx: &SessionContext,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let Some(shutdown) = Self::from_context(ctx) else {
            return input;
        };
        let schema = input.schema();
        let stream = shutdown.cancel_stream(input, || {
            exec_err!("query cancelled because the server is shutting down")
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    /// Observe shutdown at the consumer, even if the producer is still finishing a batch.
    pub fn cancel_stream<T>(
        self: Arc<Self>,
        mut input: impl Stream<Item = T> + Unpin,
        cancelled_item: impl Fn() -> T,
    ) -> impl Stream<Item = T> {
        let mut cancelled = Box::pin(self.signal.clone().cancelled_owned());
        let mut done = false;
        stream::poll_fn(move |cx| {
            if done {
                return Poll::Ready(None);
            }

            if cancelled.as_mut().poll(cx).is_pending() {
                let next = Pin::new(&mut input).poll_next(cx);
                // Shutdown can also wake the inner stream and make it report EOF.
                if !self.signal.is_cancelled() {
                    done = matches!(next, Poll::Ready(None));
                    return next;
                }
            }

            // Deliver the error before dropping the input. Its cleanup may wait for a Python UDF.
            done = true;
            Poll::Ready(Some(cancelled_item()))
        })
    }
}
