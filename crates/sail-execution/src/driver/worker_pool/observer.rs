use datafusion::common::Result;
use sail_common_datafusion::session::job::WorkerSnapshot;
use sail_common_datafusion::system::catalog::WorkerRow;
use sail_common_datafusion::system::predicate::{Predicate, PredicateExt};

use crate::driver::worker_pool::WorkerPool;
use crate::id::WorkerId;

impl WorkerPool {
    pub fn observe_workers(
        &self,
        session_id: &str,
        worker_id: Predicate<WorkerId>,
        fetch: usize,
    ) -> Result<Vec<WorkerRow>> {
        self.workers
            .iter()
            .predicate_filter_map(
                worker_id,
                |&(w, _)| w,
                |(w, worker)| worker.worker_snapshot(*w).into_row(session_id.to_string()),
            )
            .fetch(fetch)
            .collect()
    }

    pub fn observe_worker_snapshots(&self) -> Vec<WorkerSnapshot> {
        self.workers
            .iter()
            .map(|(w, worker)| worker.worker_snapshot(*w))
            .collect()
    }
}
