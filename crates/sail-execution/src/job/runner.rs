use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionConfig;

use crate::driver::DriverEngine;
use crate::error::ExecutionResult;
use crate::job::definition::JobDefinition;

pub trait JobRunner: Send + Sync + 'static {
    fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream>;
}

pub struct LocalJobRunner {}

impl LocalJobRunner {
    pub fn new() -> Self {
        Self {}
    }
}

impl JobRunner for LocalJobRunner {
    fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream> {
        // TODO: construct task context from job definition
        let ctx = TaskContext::new(
            None,
            "default".to_string(),
            SessionConfig::new(),
            Default::default(),
            Default::default(),
            Default::default(),
            Arc::new(RuntimeEnv::default()),
        );
        Ok(execute_stream(job.plan, Arc::new(ctx))?)
    }
}

pub struct ClusterJobRunner {
    driver: DriverEngine,
}

impl ClusterJobRunner {
    pub async fn start() -> ExecutionResult<Self> {
        let driver = DriverEngine::start().await?;
        Ok(Self { driver })
    }
}

impl JobRunner for ClusterJobRunner {
    fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream> {
        self.driver.execute(job)
    }
}
