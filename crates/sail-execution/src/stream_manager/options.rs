use std::time::Duration;

use crate::driver::DriverOptions;
use crate::shuffle::ShuffleBackendKind;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct StreamManagerOptions {
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub shuffle_backend: ShuffleBackendKind,
}

impl From<&DriverOptions> for StreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}

impl From<&WorkerOptions> for StreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            shuffle_backend: options.shuffle_backend.clone(),
        }
    }
}
