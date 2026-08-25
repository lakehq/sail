use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use sail_common::diagnostics::DistributedPlanV1;

use crate::extension::SessionExtension;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreparedJobHandle(u64);

impl PreparedJobHandle {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

pub struct PreparedJob {
    handle: PreparedJobHandle,
    distributed_plan: DistributedPlanV1,
}

impl PreparedJob {
    pub fn new(handle: PreparedJobHandle, distributed_plan: DistributedPlanV1) -> Self {
        Self {
            handle,
            distributed_plan,
        }
    }

    pub fn handle(&self) -> PreparedJobHandle {
        self.handle
    }

    pub fn distributed_plan(&self) -> &DistributedPlanV1 {
        &self.distributed_plan
    }

    pub fn into_parts(self) -> (PreparedJobHandle, DistributedPlanV1) {
        (self.handle, self.distributed_plan)
    }
}

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    /// Prepares a distributed job without starting it.
    async fn prepare(&self, plan: Arc<dyn ExecutionPlan>) -> Result<PreparedJob>;

    /// Executes the exact graph owned by a prepared job.
    async fn execute_prepared(
        &self,
        ctx: &SessionContext,
        job: PreparedJob,
    ) -> Result<SendableRecordBatchStream>;

    /// Releases a prepared job that will not be executed.
    async fn discard_prepared(&self, job: PreparedJob) -> Result<()>;

    /// Executes a plan.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

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
