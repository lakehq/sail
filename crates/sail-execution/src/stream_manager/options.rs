use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
pub struct StreamManagerOptions {
    pub worker_stream_buffer: usize,
}

impl StreamManagerOptions {
    pub fn new_for_driver(options: &DriverOptions) -> Self {
        Self {
            worker_stream_buffer: options.worker_stream_buffer,
        }
    }

    pub fn new_for_worker(options: &WorkerOptions) -> Self {
        Self {
            worker_stream_buffer: options.worker_stream_buffer,
        }
    }
}
