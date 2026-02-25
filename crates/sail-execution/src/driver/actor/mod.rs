mod core;
mod handler;
mod rpc;

use std::collections::HashMap;

use sail_common_datafusion::session::job::JobRunnerHistory;
use tokio::sync::oneshot;

use crate::driver::job_scheduler::JobScheduler;
use crate::driver::task_assigner::TaskAssigner;
use crate::id::{TaskKey, WorkerId};
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;

pub struct DriverActor {
    options: super::options::DriverOptions,
    server: ServerMonitor,
    worker_pool: super::worker_pool::WorkerPool,
    job_scheduler: JobScheduler,
    task_assigner: TaskAssigner,
    task_runner: TaskRunner,
    stream_manager: StreamManager,
    /// The sequence number corresponding to the last task status update from the worker.
    /// A different sequence number is tracked for each attempt.
    task_sequences: HashMap<TaskKey, u64>,
    /// Mapping from (cache_id, partition) to workers that hold the partition locally.
    ///
    /// TODO: this mapping can become stale if workers stop or evict cached partitions.
    cache_partition_locations: HashMap<(u64, usize), Vec<WorkerId>>,
    /// An optional channel to send history when stopping the driver.
    history: Option<oneshot::Sender<JobRunnerHistory>>,
}
