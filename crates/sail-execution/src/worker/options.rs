use std::time::Duration;

use datafusion::prelude::SessionContext;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::RetryStrategy;

use crate::id::WorkerId;
use crate::worker_manager::WorkerLaunchOptions;

#[readonly::make]
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
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
    pub runtime: RuntimeHandle,
    pub session: SessionContext,
}

impl WorkerOptions {
    pub fn new(config: &AppConfig, runtime: RuntimeHandle, session: SessionContext) -> Self {
        Self {
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
            task_stream_buffer: config.cluster.task_stream_buffer,
            task_stream_creation_timeout: Duration::from_secs(
                config.cluster.task_stream_creation_timeout_secs,
            ),
            rpc_retry_strategy: (&config.cluster.rpc_retry_strategy).into(),
            runtime,
            session,
        }
    }

    pub fn local(
        id: WorkerId,
        options: WorkerLaunchOptions,
        runtime: RuntimeHandle,
        session: SessionContext,
    ) -> Self {
        WorkerOptions {
            worker_id: id,
            enable_tls: options.enable_tls,
            driver_host: options.driver_external_host,
            driver_port: options.driver_external_port,
            worker_listen_host: "127.0.0.1".to_string(),
            worker_listen_port: 0,
            worker_external_host: "127.0.0.1".to_string(),
            worker_external_port: 0,
            worker_heartbeat_interval: options.worker_heartbeat_interval,
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            rpc_retry_strategy: options.rpc_retry_strategy,
            runtime: runtime.clone(),
            session,
        }
    }
}
