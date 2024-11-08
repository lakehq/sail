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
    pub worker_external_port: Option<u16>,
    pub memory_stream_buffer: usize,
}

impl TryFrom<&AppConfig> for WorkerOptions {
    type Error = ExecutionError;

    fn try_from(config: &AppConfig) -> ExecutionResult<Self> {
        Ok(Self {
            worker_id: config.worker.id.into(),
            enable_tls: config.network.enable_tls,
            driver_host: config.driver.external_host.clone(),
            driver_port: config
                .driver
                .external_port
                .unwrap_or(config.driver.listen_port),
            worker_listen_host: config.worker.listen_host.clone(),
            worker_listen_port: config.worker.listen_port,
            worker_external_host: config.worker.external_host.clone(),
            worker_external_port: config.worker.external_port,
            memory_stream_buffer: config.worker.memory_stream_buffer,
        })
    }
}
