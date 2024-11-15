use sail_common::config::AppConfig;

use crate::error::{ExecutionError, ExecutionResult};
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
    pub memory_stream_buffer: usize,
}

impl TryFrom<&AppConfig> for WorkerOptions {
    type Error = ExecutionError;

    fn try_from(config: &AppConfig) -> ExecutionResult<Self> {
        Ok(Self {
            worker_id: config.cluster.worker_id.into(),
            enable_tls: config.network.enable_tls,
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
            memory_stream_buffer: config.cluster.memory_stream_buffer,
        })
    }
}
