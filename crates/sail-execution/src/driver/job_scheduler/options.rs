// SPDX-License-Identifier: Apache-2.0

//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub session_id: String,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            session_id: options.session_id.clone(),
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
        }
    }
}
