use std::borrow::Cow;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use sail_common::telemetry::SpanAssociation;
use sail_common_datafusion::system::predicate::Predicate;
use tokio::sync::oneshot;

use crate::system_event::SystemEvent;

/// Messages accepted by the in-memory system event actor.
pub enum SystemEventActorMessage {
    Apply(SystemEvent),
    ReadJobs {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadStages {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadTasks {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadOptions {
        key: Predicate<String>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadSessions {
        session_id: Predicate<String>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadWorkers {
        session_id: Predicate<String>,
        worker_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    Shutdown,
}

impl SpanAssociation for SystemEventActorMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::Apply(_) => "Apply",
            Self::ReadJobs { .. } => "ReadJobs",
            Self::ReadStages { .. } => "ReadStages",
            Self::ReadTasks { .. } => "ReadTasks",
            Self::ReadOptions { .. } => "ReadOptions",
            Self::ReadSessions { .. } => "ReadSessions",
            Self::ReadWorkers { .. } => "ReadWorkers",
            Self::Shutdown => "Shutdown",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        []
    }
}
