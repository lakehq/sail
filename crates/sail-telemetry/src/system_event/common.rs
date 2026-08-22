use chrono::{DateTime, Utc};
use sail_common_datafusion::system::types::StageInput;
use serde::{Deserialize, Serialize};

/// The OpenTelemetry log event name used for system-table changes.
pub const SYSTEM_EVENT_NAME: &str = "sail.system";

/// A durable description of a change to a system-table row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SystemEvent {
    OptionCreated {
        key: String,
        value: String,
    },
    OptionUpdated {
        key: String,
        value: String,
    },
    SessionCreated {
        session_id: String,
        user_id: String,
        status: String,
        created_at: DateTime<Utc>,
    },
    SessionUpdated {
        session_id: String,
        status: String,
        updated_at: DateTime<Utc>,
    },
    JobCreated {
        session_id: String,
        job_id: u64,
        status: String,
        created_at: DateTime<Utc>,
    },
    JobUpdated {
        session_id: String,
        job_id: u64,
        status: String,
        updated_at: DateTime<Utc>,
    },
    StageCreated {
        session_id: String,
        job_id: u64,
        stage: u64,
        partitions: u64,
        inputs: Vec<StageInput>,
        group: String,
        mode: String,
        distribution: String,
        placement: String,
        status: String,
        created_at: DateTime<Utc>,
    },
    StageUpdated {
        session_id: String,
        job_id: u64,
        stage: u64,
        status: String,
        updated_at: DateTime<Utc>,
    },
    TaskCreated {
        session_id: String,
        job_id: u64,
        stage: u64,
        partition: u64,
        attempt: u64,
        status: String,
        created_at: DateTime<Utc>,
    },
    TaskUpdated {
        session_id: String,
        job_id: u64,
        stage: u64,
        partition: u64,
        attempt: u64,
        status: String,
        updated_at: DateTime<Utc>,
    },
    WorkerCreated {
        session_id: String,
        worker_id: u64,
        host: Option<String>,
        port: Option<u16>,
        status: String,
        created_at: DateTime<Utc>,
    },
    WorkerUpdated {
        session_id: String,
        worker_id: u64,
        host: Option<String>,
        port: Option<u16>,
        status: String,
        updated_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct OptionPrimaryKey {
    pub key: String,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionPrimaryKey {
    pub session_id: String,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct JobPrimaryKey {
    pub session_id: String,
    pub job_id: u64,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct StagePrimaryKey {
    pub session_id: String,
    pub job_id: u64,
    pub stage: u64,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct TaskPrimaryKey {
    pub session_id: String,
    pub job_id: u64,
    pub stage: u64,
    pub partition: u64,
    pub attempt: u64,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkerPrimaryKey {
    pub session_id: String,
    pub worker_id: u64,
}

pub(crate) fn is_session_deleted(status: &str) -> bool {
    status == "DELETED"
}

pub(crate) fn is_job_stopped(status: &str) -> bool {
    matches!(status, "SUCCEEDED" | "FAILED" | "CANCELED")
}

pub(crate) fn is_stage_stopped(status: &str) -> bool {
    status == "INACTIVE"
}

pub(crate) fn is_task_stopped(status: &str) -> bool {
    matches!(status, "SUCCEEDED" | "FAILED" | "CANCELED")
}

pub(crate) fn is_worker_stopped(status: &str) -> bool {
    matches!(status, "COMPLETED" | "FAILED")
}
