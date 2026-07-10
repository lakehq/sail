use std::time::Duration;

use sail_common::config::ShuffleCompression;
use sail_server::RetryStrategy;

#[derive(Debug, Clone)]
pub struct WorkerLaunchOptions {
    pub enable_tls: bool,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_heartbeat_interval: Duration,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
    pub shuffle_max_file_size: usize,
    pub shuffle_compression: ShuffleCompression,
}
