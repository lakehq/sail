// SPDX-License-Identifier: Apache-2.0

use datafusion::execution::TaskContext;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;

use crate::extension::SessionExtension;

type CreateJobContext = fn(&TaskContext, ConfigOptions) -> Result<TaskContext>;

/// Creates the task context of a job on a worker.
///
/// The worker session factory installs this extension in the worker session, since the factory
/// decides which settings stay worker-local. The worker calls it once per job with the task
/// context of the worker session and the options of the driver session that submitted the job.
pub struct JobContextFactory {
    create: CreateJobContext,
}

impl JobContextFactory {
    pub fn new(create: CreateJobContext) -> Self {
        Self { create }
    }

    pub fn create(&self, worker: &TaskContext, driver: ConfigOptions) -> Result<TaskContext> {
        (self.create)(worker, driver)
    }
}

impl SessionExtension for JobContextFactory {
    fn name() -> &'static str {
        "JobContextFactory"
    }
}
