use std::time::Duration;

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::RetryStrategy;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[derive(Debug)]
pub struct WorkerOptions {
    pub worker_id: WorkerId,
    pub enable_tls: bool,
    pub driver_host: String,
    pub driver_port: u16,
    pub worker_listen_host: String,
    pub worker_listen_port: u16,
    pub worker_external_host: String,
    pub worker_external_port: u16,
    pub worker_heartbeat_interval: Duration,
    pub worker_stream_buffer: usize,
    pub rpc_retry_strategy: RetryStrategy,
    pub runtime: RuntimeHandle,
}

impl WorkerOptions {
    pub fn try_new(config: &AppConfig, runtime: RuntimeHandle) -> ExecutionResult<Self> {
        Ok(Self {
            worker_id: config.cluster.worker_id.into(),
            enable_tls: config.cluster.enable_tls,
            driver_host: config.cluster.driver_external_host.clone(),
            driver_port: if config.cluster.driver_external_port > 0 {
                config.cluster.driver_external_port
            } else {
                config.cluster.driver_listen_port
            },
            worker_listen_host: config.cluster.worker_listen_host.clone(),
            worker_listen_port: config.cluster.worker_listen_port,
            worker_external_host: config.cluster.worker_external_host.clone(),
            worker_external_port: config.cluster.worker_external_port,
            worker_heartbeat_interval: Duration::from_secs(
                config.cluster.worker_heartbeat_interval_secs,
            ),
            worker_stream_buffer: config.cluster.worker_stream_buffer,
            rpc_retry_strategy: (&config.cluster.rpc_retry_strategy).into(),
            runtime,
        })
    }
}
