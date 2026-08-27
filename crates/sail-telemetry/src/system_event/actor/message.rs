use std::borrow::Cow;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use sail_common::telemetry::SpanAssociation;
use sail_common_datafusion::system::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use tokio::sync::oneshot;

use crate::system_event::SystemEvent;

/// Messages accepted by the in-memory system event actor.
pub enum SystemEventActorMessage {
    Apply(SystemEvent),
    ApplyMetrics {
        metrics: Vec<ResourceMetrics>,
        result: oneshot::Sender<Result<()>>,
    },
    ReadJobs {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadMetrics {
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadStages {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadTasks {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadOptions {
        key: ValueFilter<String>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadSessions {
        session_id: ValueFilter<String>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    ReadWorkers {
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<RecordBatch>>,
    },
    Shutdown,
}

impl SpanAssociation for SystemEventActorMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::Apply(_) => "Apply",
            Self::ApplyMetrics { .. } => "ApplyMetrics",
            Self::ReadJobs { .. } => "ReadJobs",
            Self::ReadMetrics { .. } => "ReadMetrics",
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
