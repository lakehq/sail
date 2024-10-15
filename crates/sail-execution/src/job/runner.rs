use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionConfig;

use crate::distributed::driver::Driver;
use crate::error::ExecutionResult;
use crate::job::definition::JobDefinition;

pub trait JobRunner {
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
    driver: Driver,
}

impl ClusterJobRunner {
    pub fn new() -> Self {
        Self {
            driver: Driver::new(),
        }
    }
}
