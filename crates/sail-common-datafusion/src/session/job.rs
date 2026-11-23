// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{TryStreamExt, stream};
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

    pub fn cancel_stream_on_shutdown(
        ctx: &SessionContext,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let Some(shutdown) = ctx.state_ref().read().config().get_extension::<Self>() else {
            return input;
        };
        let schema = input.schema();
        let stream = stream::try_unfold((input, shutdown), |(mut input, shutdown)| async move {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => exec_err!("query cancelled because the server is shutting down"),
                batch = input.try_next() => Ok(batch?.map(|batch| (batch, (input, shutdown)))),
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}
