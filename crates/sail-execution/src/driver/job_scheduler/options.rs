//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use sail_common::diagnostics::DistributedExecutionMode;

use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub execution_mode: DistributedExecutionMode,
    pub session_id: String,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_backend: ShuffleBackendKind,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            execution_mode: match options.execution_mode {
                sail_common::config::ExecutionMode::LocalCluster => {
                    DistributedExecutionMode::LocalCluster
                }
                sail_common::config::ExecutionMode::KubernetesCluster => {
                    DistributedExecutionMode::KubernetesCluster
                }
                sail_common::config::ExecutionMode::Local => {
                    unreachable!("a cluster driver cannot use local execution mode")
                }
            },
            session_id: options.session_id.clone(),
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}
