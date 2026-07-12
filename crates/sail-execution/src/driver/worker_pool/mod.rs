mod core;
mod observer;
mod options;
mod state;
mod task_resource_staging;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
pub use options::WorkerPoolOptions;
pub(crate) use task_resource_staging::{CompletedTaskResourceStaging, TaskResourceStagingId};

use crate::driver::worker_pool::state::WorkerDescriptor;
use crate::driver::worker_pool::task_resource_staging::TaskResourceStagingCache;
use crate::id::{IdGenerator, WorkerId};
use crate::worker::launch_context::TaskResourceCleanupLease;
use crate::worker_manager::WorkerManager;

pub struct WorkerPool {
    options: WorkerPoolOptions,
    driver_server_port: Option<u16>,
    worker_manager: Arc<dyn WorkerManager>,
    workers: IndexMap<WorkerId, WorkerDescriptor>,
    worker_id_generator: IdGenerator<WorkerId>,
    next_task_resource_staging_id: u64,
    task_resource_namespace: String,
    job_resources: HashMap<crate::id::JobId, JobExecutionResources>,
}

#[derive(Default)]
struct JobExecutionResources {
    cleanup_leases: HashMap<String, Vec<Arc<TaskResourceCleanupLease>>>,
    task_resource_staging: TaskResourceStagingCache,
    workers: HashSet<WorkerId>,
    /// Operations from this job epoch that can start or stop worker resource preparation.
    pending_requests: usize,
    cleanup_requested: bool,
    cleanup_in_progress: bool,
    cleanup_barrier_complete: bool,
    cleanup_retry_count: u32,
    cleanup_deadline: Option<tokio::time::Instant>,
}

impl JobExecutionResources {
    fn accepts_requests(&self) -> bool {
        !self.cleanup_requested
    }

    fn ready_for_cleanup(&self) -> bool {
        self.cleanup_requested && !self.cleanup_in_progress && self.pending_requests == 0
    }

    fn requires_worker_barrier(&self, now: tokio::time::Instant) -> bool {
        !self.cleanup_barrier_complete
            && self.cleanup_deadline.is_none_or(|deadline| deadline > now)
    }

    fn next_cleanup_retry_delay(&mut self) -> std::time::Duration {
        let exponent = self.cleanup_retry_count.min(6);
        self.cleanup_retry_count = self.cleanup_retry_count.saturating_add(1);
        std::time::Duration::from_secs(1_u64 << exponent)
    }
}

impl WorkerPool {
    pub fn new(worker_manager: Arc<dyn WorkerManager>, options: WorkerPoolOptions) -> Self {
        Self {
            options,
            driver_server_port: None,
            worker_manager,
            workers: IndexMap::new(),
            worker_id_generator: IdGenerator::new(),
            next_task_resource_staging_id: 0,
            task_resource_namespace: task_resource_namespace(),
            job_resources: HashMap::new(),
        }
    }
}

fn task_resource_namespace() -> String {
    let nanos = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    format!("{}-{nanos}", std::process::id())
}
