use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use sail_common_datafusion::extension::SessionExtension;
use sail_system_store::SystemStoreReader;
use sail_system_store::catalog::SystemTable;
use sail_system_store::predicate::{Predicates, ValueFilter};

use crate::batch::{build_metrics, build_rows};
use crate::predicate::PredicateExtractor;

pub struct SystemTableService {
    store_reader: SystemStoreReader,
    batch_size: usize,
}

impl SystemTableService {
    pub fn new(store_reader: SystemStoreReader, batch_size: usize) -> Self {
        Self {
            store_reader,
            batch_size,
        }
    }

    pub async fn read(
        &self,
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let fetch = fetch.unwrap_or(usize::MAX);
        let mut filters = PredicateExtractor::new(filters);
        let stream = match table {
            SystemTable::Jobs => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Jobs,
                    self.store_reader
                        .read_jobs(session_id, job_id, fetch)
                        .await?,
                    self.batch_size,
                    projection,
                )?
            }
            SystemTable::Metrics => {
                let timestamp = filters
                    .extract("timestamp")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let name = filters
                    .extract("name")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let attributes = filters.extract_map_values("attributes")?;
                filters.finalize()?;
                build_metrics_stream(
                    self.store_reader
                        .read_metrics(timestamp, name, attributes, fetch)
                        .await?,
                    self.batch_size,
                    projection,
                )?
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
                build_rows_stream(
                    SystemTable::Stages,
                    self.store_reader
                        .read_stages(session_id, job_id, stage, fetch)
                        .await?,
                    self.batch_size,
                    projection,
                )?
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
                build_rows_stream(
                    SystemTable::Tasks,
                    self.store_reader
                        .read_tasks(session_id, job_id, stage, partition, attempt, fetch)
                        .await?,
                    self.batch_size,
                    projection,
                )?
            }
            SystemTable::Options => {
                let key = filters
                    .extract("key")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Options,
                    self.store_reader.read_options(key, fetch).await?,
                    self.batch_size,
                    projection,
                )?
            }
            SystemTable::Sessions => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Sessions,
                    self.store_reader.read_sessions(session_id, fetch).await?,
                    self.batch_size,
                    projection,
                )?
            }
            SystemTable::Workers => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                let worker_id = filters
                    .extract("worker_id")?
                    .unwrap_or_else(|| ValueFilter::all(Predicates::always_true()));
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Workers,
                    self.store_reader
                        .read_workers(session_id, worker_id, fetch)
                        .await?,
                    self.batch_size,
                    projection,
                )?
            }
        };
        Ok(stream)
    }
}

fn projected_schema(table: SystemTable, projection: Option<&[usize]>) -> Result<SchemaRef> {
    match projection {
        Some(projection) => Ok(std::sync::Arc::new(table.schema().project(projection)?)),
        None => Ok(table.schema()),
    }
}

fn project_batch(batch: RecordBatch, projection: Option<&[usize]>) -> Result<RecordBatch> {
    match projection {
        Some(projection) => Ok(batch.project(projection)?),
        None => Ok(batch),
    }
}

fn build_rows_stream<T>(
    table: SystemTable,
    rows: Vec<T>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
) -> Result<SendableRecordBatchStream>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + 'static,
{
    let schema = projected_schema(table, projection.as_deref())?;
    let stream = futures::stream::iter(rows)
        .ready_chunks(batch_size)
        .map(move |rows| {
            build_rows(table, rows).and_then(|batch| project_batch(batch, projection.as_deref()))
        });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

fn build_metrics_stream(
    rows: Vec<sail_system_store::catalog::MetricRow>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
) -> Result<SendableRecordBatchStream> {
    let schema = projected_schema(SystemTable::Metrics, projection.as_deref())?;
    let stream = futures::stream::iter(rows)
        .ready_chunks(batch_size)
        .map(move |rows| {
            build_metrics(rows).and_then(|batch| project_batch(batch, projection.as_deref()))
        });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

impl SessionExtension for SystemTableService {
    fn name() -> &'static str {
        "SystemTableService"
    }
}
