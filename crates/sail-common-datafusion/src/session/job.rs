use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;

use crate::extension::{SessionExtension, SessionExtensionAccessor};

/// Execute an eager planning query through the session's execution backend.
/// Shared-plan lowering and execution state belong to the runner, not callers.
pub async fn collect_logical_plan(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>> {
    let service = ctx.extension::<JobService>()?;
    let physical = ctx
        .execute_logical_plan(plan)
        .await?
        .create_physical_plan()
        .await?;
    service
        .runner()
        .execute(ctx, physical)
        .await?
        .try_collect()
        .await
}

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
