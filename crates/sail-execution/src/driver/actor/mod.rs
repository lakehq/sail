mod core;
mod handler;
mod rpc;

use std::collections::HashMap;

use crate::driver::job_scheduler::{output, JobScheduler};
use crate::id::{JobId, TaskAttempt};
use crate::rpc::ServerMonitor;

pub struct DriverActor {
    options: super::options::DriverOptions,
    server: ServerMonitor,
    worker_pool: super::worker_pool::WorkerPool,
    job_scheduler: JobScheduler,
    /// The sequence number corresponding to the last task status update from the worker.
    /// A different sequence number is tracked for each attempt.
    task_sequences: HashMap<TaskAttempt, u64>,
    job_outputs: HashMap<JobId, output::JobOutput>,
}
