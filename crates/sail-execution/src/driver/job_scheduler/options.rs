//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::job_graph::TaskPlacement;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub local_execution: bool,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
}

impl JobSchedulerOptions {
    pub fn default_task_placement(&self) -> TaskPlacement {
        if self.local_execution {
            TaskPlacement::Driver
        } else {
            TaskPlacement::Worker
        }
    }
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            local_execution: options.local_execution,
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
        }
    }
}
