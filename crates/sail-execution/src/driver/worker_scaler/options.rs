use sail_common::utils::retry::RetryStrategy;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct WorkerScalerOptions {
    pub worker_launch_retry_strategy: RetryStrategy,
}

#[cfg(test)]
impl WorkerScalerOptions {
    pub(super) fn new(worker_launch_retry_strategy: RetryStrategy) -> Self {
        Self {
            worker_launch_retry_strategy,
        }
    }
}

impl From<&DriverOptions> for WorkerScalerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            worker_launch_retry_strategy: options.worker_launch_retry_strategy.clone(),
        }
    }
}
