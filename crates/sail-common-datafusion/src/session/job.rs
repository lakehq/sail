use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tokio::sync::oneshot;

use crate::extension::SessionExtension;
use crate::system::catalog::{JobRow, WorkerRow};
use crate::system::observable::{JobRunnerObserver, StateObservable};
use crate::system::predicate::PredicateExt;

#[tonic::async_trait]
pub trait JobRunner: StateObservable<JobRunnerObserver> + Send + Sync + 'static {
    /// Executes a plan
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>);
}

pub struct JobRunnerHistory {
    pub jobs: Vec<JobSnapshot>,
    pub workers: Vec<WorkerSnapshot>,
}

#[derive(Clone)]
pub struct JobSnapshot {
    pub job_id: u64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl JobSnapshot {
    pub fn into_row(self, session_id: String) -> JobRow {
        JobRow {
            session_id,
            job_id: self.job_id,
            status: self.status,
            created_at: self.created_at,
            completed_at: self.completed_at,
        }
    }
}

#[derive(Clone)]
pub struct WorkerSnapshot {
    pub worker_id: u64,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl WorkerSnapshot {
    pub fn into_row(self, session_id: String) -> WorkerRow {
        WorkerRow {
            session_id,
            worker_id: self.worker_id,
            host: self.host,
            port: self.port,
            status: self.status,
            created_at: self.created_at,
            completed_at: self.completed_at,
        }
    }
}

#[tonic::async_trait]
impl StateObservable<JobRunnerObserver> for JobRunnerHistory {
    async fn observe(&self, observer: JobRunnerObserver) {
        match observer {
            JobRunnerObserver::Jobs {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let output = self
                    .jobs
                    .iter()
                    .predicate_filter_map(
                        job_id,
                        |&job| &job.job_id,
                        |job| job.clone().into_row(session_id.clone()),
                    )
                    .fetch(fetch)
                    .collect();
                let _ = result.send(output);
            }
            JobRunnerObserver::Workers {
                session_id,
                worker_id,
                fetch,
                result,
            } => {
                let output = self
                    .workers
                    .iter()
                    .predicate_filter_map(
                        worker_id,
                        |&worker| &worker.worker_id,
                        |worker| worker.clone().into_row(session_id.clone()),
                    )
                    .fetch(fetch)
                    .collect();
                let _ = result.send(output);
            }
        }
    }
}

pub struct JobService {
    runner: Box<dyn JobRunner>,
}

impl JobService {
    pub fn new(runner: Box<dyn JobRunner>) -> Self {
        Self { runner }
    }

    pub fn runner(&self) -> &dyn JobRunner {
        self.runner.as_ref()
    }
}

impl SessionExtension for JobService {
    fn name() -> &'static str {
        "JobService"
    }
}
