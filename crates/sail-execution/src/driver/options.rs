use std::sync::Arc;
use std::time::Duration;

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::RetryStrategy;

use crate::worker_manager::WorkerManager;

#[readonly::make]
pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_initial_count: usize,
    pub worker_max_count: usize,
    pub worker_task_slots: usize,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_launch_timeout: Duration,
    pub task_launch_timeout: Duration,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub task_max_attempts: usize,
    pub job_output_buffer: usize,
    pub rpc_retry_strategy: RetryStrategy,
    pub runtime: RuntimeHandle,
    pub worker_manager: Arc<dyn WorkerManager>,
}

impl DriverOptions {
    pub fn new(
        config: &AppConfig,
        runtime: RuntimeHandle,
        worker_manager: Arc<dyn WorkerManager>,
    ) -> Self {
        Self {
            enable_tls: config.cluster.enable_tls,
            driver_listen_host: config.cluster.driver_listen_host.clone(),
            driver_listen_port: config.cluster.driver_listen_port,
            driver_external_host: config.cluster.driver_external_host.clone(),
            driver_external_port: config.cluster.driver_external_port,
            worker_initial_count: config.cluster.worker_initial_count,
            worker_max_count: config.cluster.worker_max_count,
            worker_task_slots: config.cluster.worker_task_slots,
            worker_max_idle_time: Duration::from_secs(config.cluster.worker_max_idle_time_secs),
            worker_heartbeat_interval: Duration::from_secs(
                config.cluster.worker_heartbeat_interval_secs,
            ),
            worker_heartbeat_timeout: Duration::from_secs(
                config.cluster.worker_heartbeat_timeout_secs,
            ),
            worker_launch_timeout: Duration::from_secs(config.cluster.worker_launch_timeout_secs),
            rpc_retry_strategy: (&config.cluster.rpc_retry_strategy).into(),
            task_launch_timeout: Duration::from_secs(config.cluster.task_launch_timeout_secs),
            task_stream_buffer: config.cluster.task_stream_buffer,
            task_stream_creation_timeout: Duration::from_secs(
                config.cluster.task_stream_creation_timeout_secs,
            ),
            task_max_attempts: config.cluster.task_max_attempts,
            job_output_buffer: config.cluster.job_output_buffer,
            runtime,
            worker_manager,
        }
    }
}
