use std::time::Duration;

use sail_common::config::ShuffleCompression;
use sail_common::runtime::RuntimeHandle;

use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct StreamManagerOptions {
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub runtime: RuntimeHandle,
    pub shuffle_max_file_size: usize,
    pub shuffle_compression: ShuffleCompression,
    pub shuffle_storage_url: String,
}

impl From<&DriverOptions> for StreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            runtime: options.runtime.clone(),
            shuffle_max_file_size: options.shuffle_max_file_size,
            shuffle_compression: options.shuffle_compression,
            shuffle_storage_url: options.shuffle_storage_url.clone(),
        }
    }
}

impl From<&WorkerOptions> for StreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            runtime: options.runtime.clone(),
            shuffle_max_file_size: options.shuffle_max_file_size,
            shuffle_compression: options.shuffle_compression,
            shuffle_storage_url: String::new(),
        }
    }
}
