use datafusion::common::Result;
use sail_common_datafusion::session::job::{JobSnapshot, StageSnapshot, TaskSnapshot};
use sail_common_datafusion::system::catalog::{JobRow, StageRow, TaskRow};
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
                |(j, job)| job.job_snapshot(*j).into_row(session_id.to_string()),
            )
            .fetch(fetch)
            .collect()
    }

    pub fn observe_job_snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs
            .iter()
            .map(|(j, job)| job.job_snapshot(*j))
            .collect()
    }

    pub fn observe_stages(
        &self,
        session_id: &str,
        job_id: Predicate<JobId>,
        fetch: usize,
    ) -> Result<Vec<StageRow>> {
        self.jobs
            .iter()
            .predicate_filter_flat_map(
                job_id,
                |&(j, _)| j,
                |(j, job)| {
                    job.stage_snapshots(*j)
                        .into_iter()
                        .map(|x| x.into_row(session_id.to_string()))
                },
            )
            .fetch(fetch)
            .collect()
    }

    pub fn observe_stage_snapshots(&self) -> Vec<StageSnapshot> {
        self.jobs
            .iter()
            .flat_map(|(j, job)| job.stage_snapshots(*j))
            .collect()
    }

    pub fn observe_tasks(
        &self,
        session_id: &str,
        job_id: Predicate<JobId>,
        fetch: usize,
    ) -> Result<Vec<TaskRow>> {
        self.jobs
            .iter()
            .predicate_filter_flat_map(
                job_id,
                |&(j, _)| j,
                |(j, job)| {
                    job.task_snapshots(*j)
                        .into_iter()
                        .map(|x| x.into_row(session_id.to_string()))
                },
            )
            .fetch(fetch)
            .collect()
    }

    pub fn observe_task_snapshots(&self) -> Vec<TaskSnapshot> {
        self.jobs
            .iter()
            .flat_map(|(j, job)| job.task_snapshots(*j))
            .collect()
    }
}
