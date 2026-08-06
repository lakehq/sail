use std::time::Duration;

use sail_common::utils::retry::RetryStrategy;

use crate::id::DriverId;
use crate::shuffle::ShuffleBackendKind;

#[derive(Debug, Clone)]
pub struct WorkerLaunchOptions {
    pub enable_tls: bool,
    pub session_id: String,
    pub driver_id: DriverId,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_heartbeat_interval: Duration,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub rpc_retry_strategy: RetryStrategy,
    pub shuffle_backend: ShuffleBackendKind,
}
