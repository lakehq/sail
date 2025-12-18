//! A dedicated module for worker pool options to ensure readonly access.
use std::time::Duration;

use sail_server::RetryStrategy;

use crate::driver::DriverOptions;

#[readonly::make]
pub struct WorkerPoolOptions {
    pub enable_tls: bool,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_task_slots: usize,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_stream_buffer: usize,
    pub worker_launch_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
}

impl WorkerPoolOptions {
    pub fn new(options: &DriverOptions) -> Self {
        Self {
            enable_tls: options.enable_tls,
            driver_external_host: options.driver_external_host.clone(),
            driver_external_port: options.driver_external_port,
            worker_task_slots: options.worker_task_slots,
            worker_max_idle_time: options.worker_max_idle_time,
            worker_heartbeat_interval: options.worker_heartbeat_interval,
            worker_heartbeat_timeout: options.worker_heartbeat_timeout,
            worker_stream_buffer: options.worker_stream_buffer,
            worker_launch_timeout: options.worker_launch_timeout,
            rpc_retry_strategy: options.rpc_retry_strategy.clone(),
        }
    }
}
