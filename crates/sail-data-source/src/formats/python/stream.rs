use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::runtime::SpawnedTask;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::{DataFusionError, Result};
use futures::Stream;
use tokio::sync::mpsc;

const MAX_NANOS_U64: u128 = u64::MAX as u128;

/// RecordBatch stream from Python data source.
///
/// This stream reads from a Python datasource in a dedicated thread,
/// with proper RAII cleanup via the Drop impl.
///
/// # Timeout Limitations
///
/// In-process Python execution cannot be interrupted mid-operation due to GIL
/// constraints. The stop_signal is only checked between Python `__next__` calls.
/// If a single Python operation hangs (e.g., slow network I/O in `read()`),
/// there is no way to cancel it without subprocess isolation (Phase 2/3).
///
/// Configure `python.data_source_slow_read_warn_ms` (default: 30000) to log warnings
/// when individual Python operations exceed the threshold.
use super::executor::InputPartition;

/// Metrics for Python execution performance tracking.
///
/// Tracks GIL contention and throughput to help identify bottlenecks
/// in Python datasource execution.
#[derive(Debug, Default)]
pub struct PythonExecutionMetrics {
    /// Total time spent waiting to acquire GIL (nanoseconds)
    pub gil_wait_ns: AtomicU64,
    /// Total time holding GIL (nanoseconds)
    pub gil_hold_ns: AtomicU64,
    /// Number of GIL acquisitions
    pub gil_acquisitions: AtomicU64,
    /// Number of batches processed
    pub batches_processed: AtomicU64,
    /// Total rows processed
    pub rows_processed: AtomicU64,
}

impl PythonExecutionMetrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Log a summary of the metrics.
    pub fn log_summary(&self, partition_id: usize) {
        let wait_ms = self.gil_wait_ns.load(Ordering::Relaxed) / 1_000_000;
        let hold_ms = self.gil_hold_ns.load(Ordering::Relaxed) / 1_000_000;
        let acquisitions = self.gil_acquisitions.load(Ordering::Relaxed);
        let batches = self.batches_processed.load(Ordering::Relaxed);
        let rows = self.rows_processed.load(Ordering::Relaxed);

        log::trace!(
            "[PythonDataSource:partition={}] GIL metrics: wait={}ms, hold={}ms, acquisitions={}, batches={}, rows={}",
            partition_id,
            wait_ms,
            hold_ms,
            acquisitions,
            batches,
            rows
        );

        // Log warning if GIL contention is high
        if wait_ms > hold_ms && wait_ms > 100 {
            log::warn!(
                "[PythonDataSource:partition={}] High GIL contention detected: wait={}ms > hold={}ms. \
                Consider using GIL-releasing libraries (connector-x, DuckDB) or subprocess isolation.",
                partition_id,
                wait_ms,
                hold_ms
            );
        }
    }
}

/// Default batch size for collecting rows.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// Stream state for RAII cleanup.
enum StreamState {
    Running {
        /// SpawnedTask handle - kept alive to prevent abort on drop
        /// The task runs to completion; we don't await it
        _task: SpawnedTask<()>,
        /// Receiver for batches
        rx: mpsc::Receiver<Result<RecordBatch>>,
    },
}

/// Stream that reads RecordBatches from a Python datasource.
pub struct PythonDataSourceStream {
    /// Schema of the output
    schema: SchemaRef,
    /// Stream state
    state: StreamState,
}

impl PythonDataSourceStream {
    /// Create a new stream for reading from a Python datasource.
    ///
    /// Spawns a dedicated thread for Python execution.
    ///
    /// # Arguments
    /// * `pickled_reader` - Pickled Python data source reader instance (with filters applied)
    /// * `partition` - The partition to read
    /// * `schema` - Expected output schema
    /// * `batch_size` - Batch size for row collection (from TaskContext)
    /// * `slow_read_warn_ms` - Slow read warning threshold in milliseconds (from config)
    pub fn new(
        pickled_reader: Vec<u8>,
        partition: InputPartition,
        schema: SchemaRef,
        batch_size: usize,
        slow_read_warn_ms: u64,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(16);

        let schema_clone = schema.clone();

        let task = SpawnedTask::spawn_blocking(move || {
            Self::run_python_reader(
                pickled_reader,
                partition,
                schema_clone,
                batch_size,
                slow_read_warn_ms,
                tx,
            );
        });

        Ok(Self {
            schema,
            state: StreamState::Running { _task: task, rx },
        })
    }

    /// Run the Python reader in a dedicated thread.
    fn run_python_reader(
        pickled_reader: Vec<u8>,
        partition: InputPartition,
        schema: SchemaRef,
        batch_size: usize,
        slow_read_warn_ms: u64,
        tx: mpsc::Sender<Result<RecordBatch>>,
    ) {
        use pyo3::prelude::*;
        use pyo3::types::PyBytes;

        use super::error::PythonDataSourceContext;

        let partition_id = partition.partition_id;
        let metrics = PythonExecutionMetrics::new();

        // Track time waiting for GIL
        let gil_wait_start = Instant::now();

        let result = Python::attach(|py| -> Result<()> {
            enum OutputMode {
                Arrow,
                Rows(RowBatchCollector),
            }

            // Record GIL wait time (time from start to acquiring GIL)
            let gil_acquired = Instant::now();
            metrics.gil_wait_ns.fetch_add(
                gil_wait_start.elapsed().as_nanos().min(MAX_NANOS_U64) as u64,
                Ordering::Relaxed,
            );
            metrics.gil_acquisitions.fetch_add(1, Ordering::Relaxed);

            // Deserialize reader (already has filters applied)
            let cloudpickle = import_cloudpickle(py)?;
            let reader_bytes = PyBytes::new(py, &pickled_reader);
            let reader = cloudpickle
                .call_method1("loads", (reader_bytes,))
                .map_err(py_err)?;

            let ctx = PythonDataSourceContext::new("<reader>", "read");

            // Deserialize partition
            let partition_bytes = PyBytes::new(py, &partition.data);
            let py_partition = cloudpickle
                .call_method1("loads", (partition_bytes,))
                .map_err(py_err)?;

            // Call read() directly - no need to create reader or push filters
            let iterator = reader
                .call_method1("read", (py_partition,))
                .map_err(|e| ctx.wrap_py_error(e))?;

            // Get pyarrow.RecordBatch type for isinstance check
            let pyarrow = py.import("pyarrow").map_err(py_err)?;
            let record_batch_type = pyarrow.getattr("RecordBatch").map_err(py_err)?;

            // The first value fixes the output mode, matching Spark. Build the row
            // converter lazily so Arrow-only readers retain full nested-type support.
            let mut output_mode = None;

            // Iterate over results
            loop {
                // Get next item from iterator (with slow read detection)
                let read_start = Instant::now();
                let next_result = iterator.call_method0("__next__");
                let read_elapsed_ms = read_start.elapsed().as_millis() as u64;

                if read_elapsed_ms > slow_read_warn_ms {
                    log::warn!(
                        "[PythonDataSource:partition={}] Slow read detected: {}ms (threshold: {}ms). \
                        Consider using faster I/O or subprocess isolation.",
                        partition_id,
                        read_elapsed_ms,
                        slow_read_warn_ms
                    );
                }

                match next_result {
                    Ok(item) => {
                        // Check if item is a RecordBatch (Arrow zero-copy path)
                        // or a tuple (row-based fallback path)
                        let is_record_batch = item.is_instance(&record_batch_type).unwrap_or(false);
                        if output_mode.is_none() {
                            output_mode = Some(if is_record_batch {
                                OutputMode::Arrow
                            } else {
                                OutputMode::Rows(RowBatchCollector::new(
                                    py,
                                    schema.clone(),
                                    batch_size,
                                )?)
                            });
                        }

                        if matches!(&output_mode, Some(OutputMode::Arrow)) && is_record_batch {
                            // Arrow path - zero copy transfer
                            let batch = super::arrow_utils::py_record_batch_to_rust(py, &item)?;

                            // Validate schema matches expected
                            if let Err(e) =
                                super::arrow_utils::validate_schema(&schema, batch.schema_ref())
                            {
                                let _ =
                                    tx.blocking_send(Err(with_partition_context(partition_id, e)));
                                break;
                            }

                            // Track metrics
                            metrics
                                .rows_processed
                                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                            metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                            if tx.blocking_send(Ok(batch)).is_err() {
                                break;
                            }
                        } else if let Some(OutputMode::Rows(row_batcher)) = output_mode.as_mut()
                            && !is_record_batch
                        {
                            // Tuple fallback path - retain the row until this batch is converted.
                            row_batcher.add_row(item.unbind());

                            // Flush if batch is ready
                            if row_batcher.is_ready()
                                && let Some(batch) = row_batcher.flush(py)?
                            {
                                metrics
                                    .rows_processed
                                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                                metrics.batches_processed.fetch_add(1, Ordering::Relaxed);

                                if tx.blocking_send(Ok(batch)).is_err() {
                                    break;
                                }
                            }
                        } else {
                            return Err(DataFusionError::Execution(
                                "Python data source reader cannot mix PyArrow RecordBatch and row outputs"
                                    .to_string(),
                            ));
                        }
                    }
                    Err(e) => {
                        // Check if StopIteration (normal end)
                        if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                            // Flush any remaining rows in the batcher
                            if let Some(OutputMode::Rows(row_batcher)) = output_mode.as_mut()
                                && let Some(batch) = row_batcher.flush(py)?
                            {
                                metrics
                                    .rows_processed
                                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                                metrics.batches_processed.fetch_add(1, Ordering::Relaxed);
                                let _ = tx.blocking_send(Ok(batch));
                            }
                            break;
                        }
                        // Other error - wrap with partition context
                        let _ =
                            tx.blocking_send(Err(with_partition_context(partition_id, py_err(e))));
                        break;
                    }
                }
            }

            // Record total GIL hold time
            metrics.gil_hold_ns.fetch_add(
                gil_acquired.elapsed().as_nanos().min(MAX_NANOS_U64) as u64,
                Ordering::Relaxed,
            );

            Ok(())
        });

        // Log metrics summary
        metrics.log_summary(partition_id);

        // Send any error with partition context
        if let Err(e) = result {
            let _ = tx.blocking_send(Err(with_partition_context(partition_id, e)));
        }
    }
}

impl Stream for PythonDataSourceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            StreamState::Running { rx, .. } => Pin::new(rx).poll_recv(cx),
        }
    }
}

impl RecordBatchStream for PythonDataSourceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Re-export py_err and import_cloudpickle from error module.
use super::error::{import_cloudpickle, py_err};

/// Wrap an error with partition context for better debugging.
fn with_partition_context(
    partition_id: usize,
    err: datafusion_common::DataFusionError,
) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Context(
        format!("partition {}", partition_id),
        Box::new(err),
    )
}

/// Helper for collecting rows into batches.
pub struct RowBatchCollector {
    converter: super::arrow_utils::RowReaderConverter,
    /// Collected Python tuples.
    rows: Vec<pyo3::Py<pyo3::PyAny>>,
    /// Batch size threshold
    batch_size: usize,
}

impl RowBatchCollector {
    /// Create a new collector.
    pub fn new(py: pyo3::Python<'_>, schema: SchemaRef, batch_size: usize) -> Result<Self> {
        Ok(Self {
            converter: super::arrow_utils::RowReaderConverter::try_new(py, schema)?,
            rows: Vec::with_capacity(batch_size),
            batch_size,
        })
    }

    /// Add a Python row tuple.
    pub fn add_row(&mut self, row: pyo3::Py<pyo3::PyAny>) {
        self.rows.push(row);
    }

    /// Check if batch is ready.
    pub fn is_ready(&self) -> bool {
        self.rows.len() >= self.batch_size
    }

    /// Flush collected rows to a batch.
    pub fn flush(&mut self, py: pyo3::Python<'_>) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let rows = std::mem::take(&mut self.rows);
        let batch = self.converter.convert_rows(py, &rows)?;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_row_batch_collector() {
        pyo3::Python::initialize();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        pyo3::Python::attach(|py| {
            let mut collector = RowBatchCollector::new(py, schema, 100).unwrap();

            assert!(!collector.is_ready());

            for _ in 0..50 {
                collector.add_row(py.None());
            }
            assert!(!collector.is_ready());

            for _ in 0..60 {
                collector.add_row(py.None());
            }
            assert!(collector.is_ready());
        });
    }
}
