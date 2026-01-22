use datafusion::common::Result;
use sail_common_datafusion::session::job::JobSnapshot;
use sail_common_datafusion::system::catalog::JobRow;
use sail_common_datafusion::system::predicate::{Predicate, PredicateExt};

use crate::driver::job_scheduler::JobScheduler;
use crate::id::JobId;

impl JobScheduler {
    pub fn observe_jobs(
        &self,
        session_id: &str,
        job_id: Predicate<JobId>,
        fetch: usize,
    ) -> Result<Vec<JobRow>> {
        self.jobs
            .iter()
            .predicate_filter_map(
                job_id,
                |&(j, _)| j,
                |(j, job)| job.snapshot(*j).into_row(session_id.to_string()),
            )
            .fetch(fetch)
            .collect()
    }

    pub fn observe_job_snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs.iter().map(|(j, job)| job.snapshot(*j)).collect()
    }
}
