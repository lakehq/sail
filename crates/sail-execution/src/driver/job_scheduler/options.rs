//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub task_launch_timeout: Duration,
}

impl JobSchedulerOptions {
    pub fn new(options: &DriverOptions) -> Self {
        Self {
            task_launch_timeout: options.task_launch_timeout,
        }
    }
}
