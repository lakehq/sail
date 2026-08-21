use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::system::catalog::{
    JobRow, OptionRow, SessionRow, StageRow, SystemTable, TaskRow, WorkerRow,
};
use sail_common_datafusion::system::predicate::Predicate;
use serde::{Deserialize, Serialize};

use super::{SystemEventActor, SystemEventStore};
use crate::system_event::{
    JobPrimaryKey, OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, SystemEvent,
    TaskPrimaryKey, WorkerPrimaryKey,
};

impl SystemEventActor {
    pub(super) fn read_jobs(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.jobs.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Jobs, rows)
    }

    pub(super) fn read_stages(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.stages.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Stages, rows)
    }

    pub(super) fn read_tasks(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.tasks.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Tasks, rows)
    }

    pub(super) fn read_options(&self, key: Predicate<String>, fetch: usize) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.options.values() {
            if rows.len() >= fetch {
                break;
            }
            if key(&row.key)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Options, rows)
    }

    pub(super) fn read_sessions(
        &self,
        session_id: Predicate<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.sessions.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Sessions, rows)
    }

    pub(super) fn read_workers(
        &self,
        session_id: Predicate<String>,
        worker_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.workers.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && worker_id(&row.worker_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Workers, rows)
    }

    fn build_batch<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
    }
}

impl SystemEventStore {
    pub(super) fn apply(&mut self, event: SystemEvent) {
        match event {
            SystemEvent::OptionCreated { key, value }
            | SystemEvent::OptionUpdated { key, value } => {
                self.options.insert(
                    OptionPrimaryKey { key: key.clone() },
                    OptionRow { key, value },
                );
            }
            SystemEvent::SessionCreated {
                session_id,
                user_id,
                created_at,
            } => {
                self.sessions
                    .entry(SessionPrimaryKey {
                        session_id: session_id.clone(),
                    })
                    .or_insert(SessionRow {
                        session_id,
                        user_id,
                        status: "CREATED".into(),
                        created_at,
                        deleted_at: None,
                    });
            }
            SystemEvent::SessionUpdated {
                session_id,
                status,
                updated_at,
            } => {
                if let Some(row) = self.sessions.get_mut(&SessionPrimaryKey { session_id }) {
                    row.status = status;
                    if row.deleted_at.is_none() && is_session_deleted(&row.status) {
                        row.deleted_at = Some(updated_at);
                    }
                }
            }
            SystemEvent::JobCreated {
                session_id,
                job_id,
                created_at,
            } => {
                self.jobs
                    .entry(JobPrimaryKey {
                        session_id: session_id.clone(),
                        job_id,
                    })
                    .or_insert(JobRow {
                        session_id,
                        job_id,
                        status: "CREATED".into(),
                        created_at,
                        stopped_at: None,
                    });
            }
            SystemEvent::JobUpdated {
                session_id,
                job_id,
                status,
                updated_at,
            } => {
                if let Some(row) = self.jobs.get_mut(&JobPrimaryKey { session_id, job_id }) {
                    row.status = status;
                    if row.stopped_at.is_none() && is_job_stopped(&row.status) {
                        row.stopped_at = Some(updated_at);
                    }
                }
            }
            SystemEvent::StageCreated {
                session_id,
                job_id,
                stage,
                partitions,
                inputs,
                group,
                mode,
                distribution,
                placement,
                created_at,
            } => {
                self.stages
                    .entry(StagePrimaryKey {
                        session_id: session_id.clone(),
                        job_id,
                        stage,
                    })
                    .or_insert(StageRow {
                        session_id,
                        job_id,
                        stage,
                        partitions,
                        inputs,
                        group,
                        mode,
                        distribution,
                        placement,
                        status: "ACTIVE".into(),
                        created_at,
                        stopped_at: None,
                    });
            }
            SystemEvent::StageUpdated {
                session_id,
                job_id,
                stage,
                status,
                updated_at,
            } => {
                if let Some(row) = self.stages.get_mut(&StagePrimaryKey {
                    session_id,
                    job_id,
                    stage,
                }) {
                    row.status = status;
                    if row.stopped_at.is_none() && is_stage_stopped(&row.status) {
                        row.stopped_at = Some(updated_at);
                    }
                }
            }
            SystemEvent::TaskCreated {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                created_at,
            } => {
                self.tasks
                    .entry(TaskPrimaryKey {
                        session_id: session_id.clone(),
                        job_id,
                        stage,
                        partition,
                        attempt,
                    })
                    .or_insert(TaskRow {
                        session_id,
                        job_id,
                        stage,
                        partition,
                        attempt,
                        status: "CREATED".into(),
                        created_at,
                        stopped_at: None,
                    });
            }
            SystemEvent::TaskUpdated {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                status,
                updated_at,
            } => {
                if let Some(row) = self.tasks.get_mut(&TaskPrimaryKey {
                    session_id,
                    job_id,
                    stage,
                    partition,
                    attempt,
                }) {
                    row.status = status;
                    if row.stopped_at.is_none() && is_task_stopped(&row.status) {
                        row.stopped_at = Some(updated_at);
                    }
                }
            }
            SystemEvent::WorkerCreated {
                session_id,
                worker_id,
                created_at,
            } => {
                self.workers
                    .entry(WorkerPrimaryKey {
                        session_id: session_id.clone(),
                        worker_id,
                    })
                    .or_insert(WorkerRow {
                        session_id,
                        worker_id,
                        host: None,
                        port: None,
                        status: "PENDING".into(),
                        created_at,
                        stopped_at: None,
                    });
            }
            SystemEvent::WorkerUpdated {
                session_id,
                worker_id,
                host,
                port,
                status,
                updated_at,
            } => {
                if let Some(row) = self.workers.get_mut(&WorkerPrimaryKey {
                    session_id,
                    worker_id,
                }) {
                    row.host = host;
                    row.port = port;
                    row.status = status;
                    if row.stopped_at.is_none() && is_worker_stopped(&row.status) {
                        row.stopped_at = Some(updated_at);
                    }
                }
            }
        }
    }
}

fn is_session_deleted(status: &str) -> bool {
    status == "DELETED"
}

fn is_job_stopped(status: &str) -> bool {
    matches!(status, "SUCCEEDED" | "FAILED" | "CANCELED")
}

fn is_stage_stopped(status: &str) -> bool {
    status == "INACTIVE"
}

fn is_task_stopped(status: &str) -> bool {
    matches!(status, "SUCCEEDED" | "FAILED" | "CANCELED")
}

fn is_worker_stopped(status: &str) -> bool {
    matches!(status, "COMPLETED" | "FAILED")
}
