//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
        }
    }
}
