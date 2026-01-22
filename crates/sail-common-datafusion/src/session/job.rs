use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tokio::sync::oneshot;

use crate::extension::SessionExtension;
use crate::system::catalog::{JobRow, StageRow, TaskRow, WorkerRow};
use crate::system::observable::{JobRunnerObserver, StateObservable};
use crate::system::predicate::PredicateExt;
use crate::system::types::StageInput;

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
    pub stages: Vec<StageSnapshot>,
    pub tasks: Vec<TaskSnapshot>,
    pub workers: Vec<WorkerSnapshot>,
}

#[derive(Clone)]
pub struct JobSnapshot {
    pub job_id: u64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

impl JobSnapshot {
    pub fn into_row(self, session_id: String) -> JobRow {
        let Self {
            job_id,
            status,
            created_at,
            stopped_at,
        } = self;
        JobRow {
            session_id,
            job_id,
            status,
            created_at,
            stopped_at,
        }
    }
}

#[derive(Clone)]
pub struct StageSnapshot {
    pub job_id: u64,
    pub stage: u64,
    pub partitions: u64,
    pub inputs: Vec<StageInput>,
    pub group: String,
    pub mode: String,
    pub distribution: String,
    pub placement: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

impl StageSnapshot {
    pub fn into_row(self, session_id: String) -> StageRow {
        let Self {
            job_id,
            stage,
            partitions,
            inputs,
            group,
            mode,
            distribution,
            placement,
            status,
            created_at,
            stopped_at,
        } = self;
        StageRow {
            session_id,
            job_id,
            stage,
            partitions,
            inputs,
            group,
            mode,
            distribution,
            placement,
            status,
            created_at,
            stopped_at,
        }
    }
}

#[derive(Clone)]
pub struct TaskSnapshot {
    pub job_id: u64,
    pub stage: u64,
    pub partition: u64,
    pub attempt: u64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

impl TaskSnapshot {
    pub fn into_row(self, session_id: String) -> TaskRow {
        let Self {
            job_id,
            stage,
            partition,
            attempt,
            status,
            created_at,
            stopped_at,
        } = self;
        TaskRow {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            status,
            created_at,
            stopped_at,
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
    pub stopped_at: Option<DateTime<Utc>>,
}

impl WorkerSnapshot {
    pub fn into_row(self, session_id: String) -> WorkerRow {
        let Self {
            worker_id,
            host,
            port,
            status,
            created_at,
            stopped_at,
        } = self;
        WorkerRow {
            session_id,
            worker_id,
            host,
            port,
            status,
            created_at,
            stopped_at,
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
            JobRunnerObserver::Stages {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let output = self
                    .stages
                    .iter()
                    .predicate_filter_map(
                        job_id,
                        |&stage| &stage.job_id,
                        |stage| stage.clone().into_row(session_id.clone()),
                    )
                    .fetch(fetch)
                    .collect();
                let _ = result.send(output);
            }
            JobRunnerObserver::Tasks {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let output = self
                    .tasks
                    .iter()
                    .predicate_filter_map(
                        job_id,
                        |&task| &task.job_id,
                        |task| task.clone().into_row(session_id.clone()),
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
