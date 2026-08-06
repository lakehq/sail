mod core;
mod handler;

use std::collections::HashMap;

use sail_common_datafusion::session::job::JobRunnerHistoryReporter;
use tokio::sync::oneshot;

use crate::driver::job_scheduler::JobScheduler;
use crate::driver::task_assigner::TaskAssigner;
use crate::id::TaskKey;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;

pub struct DriverActor {
    options: super::options::DriverOptions,
    history_reporter: Box<dyn JobRunnerHistoryReporter>,
    worker_pool: super::worker_pool::WorkerPool,
    job_scheduler: JobScheduler,
    task_assigner: TaskAssigner,
    task_runner: TaskRunner,
    stream_manager: StreamManager,
    /// The sequence number corresponding to the last task status update from the worker.
    /// A different sequence number is tracked for each attempt.
    task_sequences: HashMap<TaskKey, u64>,
    /// An optional channel to signal that the driver has stopped.
    shutdown_notifier: Option<oneshot::Sender<()>>,
}
