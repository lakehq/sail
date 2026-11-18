// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct LocalStreamManagerOptions {
    pub shuffle_write_buffer_size: usize,
    pub task_stream_memory_limit: usize,
    pub task_stream_stall_timeout: Duration,
    pub task_stream_creation_timeout: Duration,
}

impl From<&DriverOptions> for LocalStreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            shuffle_write_buffer_size: options.shuffle_write_buffer_size,
            task_stream_memory_limit: options.task_stream_memory_limit,
            task_stream_stall_timeout: options.task_stream_stall_timeout,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}

impl From<&WorkerOptions> for LocalStreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            shuffle_write_buffer_size: options.shuffle_write_buffer_size,
            task_stream_memory_limit: options.task_stream_memory_limit,
            task_stream_stall_timeout: options.task_stream_stall_timeout,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
        }
    }
}
