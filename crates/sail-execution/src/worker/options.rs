use sail_common::config::AppConfig;

use crate::id::WorkerId;

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

impl From<&AppConfig> for WorkerOptions {
    fn from(config: &AppConfig) -> Self {
        WorkerOptions {
            worker_id: config.worker.id.into(),
            enable_tls: config.network.enable_tls,
            driver_host: config.driver.listen_host.clone(),
            driver_port: config.driver.listen_port,
            worker_listen_host: config.worker.listen_host.clone(),
            worker_listen_port: config.worker.listen_port,
            worker_external_host: config.worker.external_host.clone(),
            worker_external_port: config.worker.external_port,
            memory_stream_buffer: config.worker.memory_stream_buffer,
        }
    }
}
