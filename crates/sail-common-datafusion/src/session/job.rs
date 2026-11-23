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

/// Shared by all query requests and result streams on one server.
#[derive(Default)]
pub struct ServerShutdown {
    signal: CancellationToken,
}

impl ServerShutdown {
    pub fn cancel(&self) {
        self.signal.cancel();
    }

    pub async fn cancelled(&self) {
        self.signal.cancelled().await;
    }

    pub fn from_context(ctx: &SessionContext) -> Option<Arc<Self>> {
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
        let mut cancelled = Box::pin(async move { self.cancelled().await });
        let mut done = false;
        stream::poll_fn(move |cx| {
            if done {
                return Poll::Ready(None);
            }

            if cancelled.as_mut().poll(cx).is_ready() {
                // Deliver the error before dropping the input. Its cleanup may wait for a Python UDF.
                done = true;
                return Poll::Ready(Some(cancelled_item()));
            }

            let next = Pin::new(&mut input).poll_next(cx);
            done = matches!(next, Poll::Ready(None));
            next
        })
    }
}
