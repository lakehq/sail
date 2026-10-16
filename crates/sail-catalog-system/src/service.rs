use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryStreamExt;
use sail_common_datafusion::extension::SessionExtension;
use sail_system_store::catalog::SystemTable;
use sail_system_store::{SystemStoreReader, SystemStoreResult};
use tokio::sync::mpsc;

use crate::batch::{SystemTableRow, build_rows};
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
                let session_id = filters.extract_or_default("session_id")?;
                let job_id = filters.extract_or_default("job_id")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Jobs,
                    self.store_reader
                        .read_jobs(session_id, job_id, fetch, self.batch_size)?,
                    projection,
                )?
            }
            SystemTable::Metrics => {
                let timestamp = filters.extract_or_default("timestamp")?;
                let name = filters.extract_or_default("name")?;
                let attributes = filters.extract_map_values("attributes")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Metrics,
                    self.store_reader.read_metrics(
                        timestamp,
                        name,
                        attributes,
                        fetch,
                        self.batch_size,
                    )?,
                    projection,
                )?
            }
            SystemTable::Stages => {
                let session_id = filters.extract_or_default("session_id")?;
                let job_id = filters.extract_or_default("job_id")?;
                let stage = filters.extract_or_default("stage")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Stages,
                    self.store_reader.read_stages(
                        session_id,
                        job_id,
                        stage,
                        fetch,
                        self.batch_size,
                    )?,
                    projection,
                )?
            }
            SystemTable::Tasks => {
                let session_id = filters.extract_or_default("session_id")?;
                let job_id = filters.extract_or_default("job_id")?;
                let stage = filters.extract_or_default("stage")?;
                let partition = filters.extract_or_default("partition")?;
                let attempt = filters.extract_or_default("attempt")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Tasks,
                    self.store_reader.read_tasks(
                        session_id,
                        job_id,
                        stage,
                        partition,
                        attempt,
                        fetch,
                        self.batch_size,
                    )?,
                    projection,
                )?
            }
            SystemTable::Options => {
                let key = filters.extract_or_default("key")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Options,
                    self.store_reader
                        .read_options(key, fetch, self.batch_size)?,
                    projection,
                )?
            }
            SystemTable::Sessions => {
                let session_id = filters.extract_or_default("session_id")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Sessions,
                    self.store_reader
                        .read_sessions(session_id, fetch, self.batch_size)?,
                    projection,
                )?
            }
            SystemTable::Workers => {
                let session_id = filters.extract_or_default("session_id")?;
                let worker_id = filters.extract_or_default("worker_id")?;
                filters.finalize()?;
                build_rows_stream(
                    SystemTable::Workers,
                    self.store_reader.read_workers(
                        session_id,
                        worker_id,
                        fetch,
                        self.batch_size,
                    )?,
                    projection,
                )?
            }
        };
        Ok(stream)
    }
}

fn projected_schema(table: SystemTable, projection: Option<&[usize]>) -> Result<SchemaRef> {
    match projection {
        Some(projection) => Ok(Arc::new(table.schema().project(projection)?)),
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
    rows: mpsc::Receiver<SystemStoreResult<Option<Vec<T>>>>,
    projection: Option<Vec<usize>>,
) -> Result<SendableRecordBatchStream>
where
    T: SystemTableRow + Send + 'static,
{
    let schema = projected_schema(table, projection.as_deref())?;
    let stream = futures::stream::try_unfold(rows, |mut rows| async move {
        match rows.recv().await {
            Some(Ok(Some(batch))) => Ok(Some((batch, rows))),
            Some(Ok(None)) => Ok(None),
            Some(Err(error)) => Err(DataFusionError::External(Box::new(error))),
            None => Err(DataFusionError::Internal(
                "system store read ended before reporting completion".to_string(),
            )),
        }
    });
    let stream = stream.and_then(move |rows| {
        futures::future::ready(
            build_rows(table, rows).and_then(|batch| project_batch(batch, projection.as_deref())),
        )
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

impl SessionExtension for SystemTableService {
    fn name() -> &'static str {
        "SystemTableService"
    }
}
