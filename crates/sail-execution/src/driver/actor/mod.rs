mod core;
mod handler;
mod rpc;

use std::collections::HashMap;

use sail_common_datafusion::cache_manager::CacheId;
use sail_common_datafusion::session::job::JobRunnerHistory;
use tokio::sync::oneshot;

use crate::driver::job_scheduler::CachePinError;
use crate::driver::job_scheduler::JobScheduler;
use crate::driver::task_assigner::TaskAssigner;
use crate::id::{TaskKey, WorkerId};
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;

/// Tracks worker locations for cached partitions.
#[derive(Clone, Default)]
struct CachePartitionLocations {
    entries: HashMap<(CacheId, usize), Vec<WorkerId>>,
}

impl CachePartitionLocations {
    /// Records that a worker stores a cache partition.
    fn record(&mut self, cache_id: CacheId, partition: usize, worker_id: WorkerId) {
        let entry = self.entries.entry((cache_id, partition)).or_default();
        if !entry.contains(&worker_id) {
            entry.push(worker_id);
        }
    }

    /// Resolves the unique worker location for a cache partition.
    fn resolve_worker(
        &self,
        cache_id: CacheId,
        partition: usize,
    ) -> Result<WorkerId, CachePinError> {
        let workers =
            self.entries
                .get(&(cache_id, partition))
                .ok_or(CachePinError::MissingLocation {
                    cache_id,
                    partition,
                })?;
        match workers.as_slice() {
            [] => Err(CachePinError::MissingLocation {
                cache_id,
                partition,
            }),
            [worker_id] => {
                let driver_worker_id = WorkerId::from(0_u64);
                if *worker_id == driver_worker_id {
                    Err(CachePinError::DriverLocationForWorkerTask {
                        cache_id,
                        partition,
                    })
                } else {
                    Ok(*worker_id)
                }
            }
            _ => Err(CachePinError::MultipleLocations {
                cache_id,
                partition,
                workers: workers.len(),
            }),
        }
    }
}

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
    cache_partition_locations: CachePartitionLocations,
    /// An optional channel to send history when stopping the driver.
    history: Option<oneshot::Sender<JobRunnerHistory>>,
}
