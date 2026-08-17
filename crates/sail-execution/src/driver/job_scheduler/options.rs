//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_backend: ShuffleBackendKind,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}
