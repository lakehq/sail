use std::collections::BTreeMap;

use sail_common_datafusion::system::catalog::{
    JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow,
};

use crate::system_event::common::{
    is_job_stopped, is_session_deleted, is_stage_stopped, is_task_stopped, is_worker_stopped,
};
use crate::system_event::{
    JobPrimaryKey, OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, SystemEvent,
    TaskPrimaryKey, WorkerPrimaryKey,
};

#[derive(Default)]
pub(super) struct SystemEventStore {
    pub(super) options: BTreeMap<OptionPrimaryKey, OptionRow>,
    pub(super) sessions: BTreeMap<SessionPrimaryKey, SessionRow>,
    pub(super) jobs: BTreeMap<JobPrimaryKey, JobRow>,
    pub(super) stages: BTreeMap<StagePrimaryKey, StageRow>,
    pub(super) tasks: BTreeMap<TaskPrimaryKey, TaskRow>,
    pub(super) workers: BTreeMap<WorkerPrimaryKey, WorkerRow>,
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
                status,
                created_at,
            } => {
                self.sessions
                    .entry(SessionPrimaryKey {
                        session_id: session_id.clone(),
                    })
                    .or_insert(SessionRow {
                        session_id,
                        user_id,
                        status,
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
                status,
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
                        status,
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
                status,
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
                        status,
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
                status,
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
                        status,
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
                host,
                port,
                status,
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
                        host,
                        port,
                        status,
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
