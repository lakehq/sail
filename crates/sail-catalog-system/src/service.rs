use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use sail_common_datafusion::extension::SessionExtension;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::system::predicate::{Predicates, ValueFilter};
use sail_telemetry::system_event::SystemEventReader;

use crate::predicate::PredicateExtractor;

pub struct SystemTableService {
    event_reader: SystemEventReader,
}

impl SystemTableService {
    pub fn new(event_reader: SystemEventReader) -> Self {
        Self { event_reader }
    }

    pub async fn read(
        &self,
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: Option<usize>,
    ) -> Result<RecordBatch> {
        let fetch = fetch.unwrap_or(usize::MAX);
        let mut filters = PredicateExtractor::new(filters);
        let batch = match table {
            SystemTable::Jobs => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader
                    .read_jobs(session_id, job_id, fetch)
                    .await?
            }
            SystemTable::Stages => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let stage = filters
                    .extract("stage")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader
                    .read_stages(session_id, job_id, stage, fetch)
                    .await?
            }
            SystemTable::Tasks => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let stage = filters
                    .extract("stage")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let partition = filters
                    .extract("partition")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let attempt = filters
                    .extract("attempt")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader
                    .read_tasks(session_id, job_id, stage, partition, attempt, fetch)
                    .await?
            }
            SystemTable::Options => {
                let key = filters
                    .extract("key")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader.read_options(key, fetch).await?
            }
            SystemTable::Sessions => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader.read_sessions(session_id, fetch).await?
            }
            SystemTable::Workers => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let worker_id = filters
                    .extract("worker_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                self.event_reader
                    .read_workers(session_id, worker_id, fetch)
                    .await?
            }
        };
        if let Some(projection) = projection {
            Ok(batch.project(&projection)?)
        } else {
            Ok(batch)
        }
    }
}

impl SessionExtension for SystemTableService {
    fn name() -> &'static str {
        "SystemTableService"
    }
}
