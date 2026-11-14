use std::time::Duration;

use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct LocalStreamManagerOptions {
    pub batch_size: usize,
    pub task_stream_buffer: usize,
    pub task_stream_memory_limit: usize,
    pub task_stream_creation_timeout: Duration,
}

impl From<&DriverOptions> for LocalStreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            batch_size: options.batch_size,
            task_stream_buffer: options.task_stream_buffer,
            task_stream_memory_limit: options.task_stream_memory_limit,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}

impl From<&WorkerOptions> for LocalStreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            batch_size: options.batch_size,
            task_stream_buffer: options.task_stream_buffer,
            task_stream_memory_limit: options.task_stream_memory_limit,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}
