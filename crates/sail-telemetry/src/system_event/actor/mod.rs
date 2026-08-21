mod core;
mod handler;
mod message;

use std::collections::BTreeMap;

pub use message::SystemEventActorMessage;
use sail_common_datafusion::system::catalog::{
    JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow,
};

use crate::system_event::{
    JobPrimaryKey, OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, TaskPrimaryKey,
    WorkerPrimaryKey,
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

/// Owns the materialized system-table rows and applies system-event CRUD messages serially.
#[derive(Default)]
pub struct SystemEventActor {
    pub(super) store: SystemEventStore,
}
