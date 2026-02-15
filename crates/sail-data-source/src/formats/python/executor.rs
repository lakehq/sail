/// PythonExecutor trait for abstracting Python execution.
///
/// This trait enables easy swapping between:
/// - `InProcessExecutor`: Direct PyO3 calls (MVP)
/// - `RemoteExecutor`: Subprocess isolation via gRPC (PR #3)
///
/// The abstraction ensures we can add subprocess isolation without rewriting core logic.
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result;
use futures::stream::BoxStream;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

use super::error::{import_cloudpickle, PythonDataSourceContext};
use super::filter::{filters_to_python, PythonFilter};

/// Default capacity for the write channel, matching `python.data_source_write_channel_capacity` config.
const DEFAULT_WRITE_CHANNEL_CAPACITY: usize = 8;

/// Default slow write warning threshold in milliseconds, matching `python.data_source_slow_write_warn_ms` config.
const DEFAULT_SLOW_WRITE_WARN_MS: u64 = 30_000;

/// Default slow read warning threshold in milliseconds, matching `python.data_source_slow_read_warn_ms` config.
const DEFAULT_SLOW_READ_WARN_MS: u64 = 30_000;

/// Input partition for parallel reading.
#[derive(Debug, Clone)]
pub struct InputPartition {
    /// Partition identifier
    pub partition_id: usize,
    /// Pickled partition data (opaque to Rust)
    pub data: Vec<u8>,
}

impl InputPartition {
    /// Get the size of the partition data in bytes.
    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }
}

/// Result of partition planning, containing the pickled reader and partitions.
///
/// The reader is pickled with filters already applied, so execution only needs
/// to deserialize and call `read(partition)` without re-applying filters.
#[derive(Debug, Clone)]
pub struct PartitionPlan {
    /// Pickled Python data source reader instance (with filters applied)
    pub pickled_reader: Vec<u8>,
    /// Partitions for parallel reading
    pub partitions: Vec<InputPartition>,
}

/// Result of writer setup, containing the pickled writer and its type.
///
/// The writer is pickled after calling `datasource.writer(schema, overwrite)`.
/// Execution will deserialize and call `write(iterator)` on it.
#[derive(Debug, Clone)]
pub struct WriterPlan {
    /// Pickled Python data source writer instance
    pub pickled_writer: Vec<u8>,
    /// Whether this is an Arrow-based writer (DataSourceArrowWriter)
    /// If false, it's a Row-based writer (DataSourceWriter)
    pub is_arrow: bool,
}

/// Result of a write operation from a single partition.
///
/// Contains the optional commit message returned by `writer.write(iterator)`.
/// These messages are collected and passed to `writer.commit()` or `writer.abort()`.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Optional commit message from writer.write()
    /// This is pickled bytes that will be unpickled in commit/abort
    pub commit_message: Option<Vec<u8>>,
}

/// Abstract executor for Python datasource operations.
///
/// This trait provides the abstraction layer between Sail's execution engine
/// and Python datasource implementations. The MVP uses `InProcessExecutor`,
/// but the trait is designed for future `RemoteExecutor` implementation.
#[async_trait]
pub trait PythonExecutor: Send + Sync + std::fmt::Debug {
    /// Get the schema from the Python datasource.
    ///
    /// Calls the Python `DataSource.schema()` method.
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef>;

    /// Get partitions for parallel reading.
    ///
    /// Calls the Python `DataSource.reader(schema).partitions()` method.
    /// If filters are provided, calls `pushFilters()` on the reader first.
    /// Returns a `PartitionPlan` containing the pickled reader (with filters applied)
    /// and the list of partitions.
    async fn get_partitions(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        filters: Vec<PythonFilter>,
    ) -> Result<PartitionPlan>;

    /// Execute a read for a specific partition.
    ///
    /// Takes a pickled reader (with filters already applied) and partition.
    /// Returns a stream of RecordBatches from Python.
    async fn execute_read(
        &self,
        pickled_reader: &[u8],
        partition: &InputPartition,
        schema: SchemaRef,
        batch_size: usize,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;

    /// Get a writer for writing data to the Python datasource.
    ///
    /// Calls `DataSource.writer(schema, overwrite)` and returns a pickled writer.
    /// Also performs isinstance checks to determine if it's Arrow or Row-based.
    async fn get_writer(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        overwrite: bool,
    ) -> Result<WriterPlan>;

    /// Execute a write operation with a stream of RecordBatches.
    ///
    /// Calls `writer.write(iterator)` where iterator yields either:
    /// - PyArrow RecordBatch objects (if is_arrow=true)
    /// - PySpark `Row` objects (if is_arrow=false)
    async fn execute_write(
        &self,
        pickled_writer: &[u8],
        schema: SchemaRef,
        is_arrow: bool,
        batches: BoxStream<'static, Result<RecordBatch>>,
    ) -> Result<WriteResult>;

    /// Commit a write operation.
    ///
    /// Calls `writer.commit(messages)` with all commit messages from partitions.
    /// `None` entries (from partitions that returned no commit message) are
    /// passed as Python `None` in the messages list.
    async fn commit_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;

    /// Abort a write operation.
    ///
    /// Calls `writer.abort(messages)` with all commit messages from partitions.
    /// Errors from abort are logged but not propagated (best-effort cleanup).
    async fn abort_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;
}

/// In-process executor using PyO3.
///
/// This is the MVP implementation that calls Python directly via PyO3.
/// GIL contention is acceptable for control plane operations (schema, partitions).
/// Data plane uses Arrow C Data Interface for efficiency.
///
/// Note: Python version validation happens in PythonDataSource::new(), not here.
#[derive(Debug, Clone)]
pub struct InProcessExecutor {
    /// Channel capacity for streaming RecordBatches to Python during write.
    write_channel_capacity: usize,
    /// Slow write warning threshold in milliseconds.
    slow_write_warn_ms: u64,
    /// Slow read warning threshold in milliseconds.
    slow_read_warn_ms: u64,
}

impl InProcessExecutor {
    /// Create a new in-process executor with default config values.
    ///
    /// For production use, prefer `from_app_config()` to load values from
    /// the application config (`python.data_source_write_channel_capacity`,
    /// `python.data_source_slow_write_warn_ms`, `python.data_source_slow_read_warn_ms`).
    pub fn new() -> Self {
        Self {
            write_channel_capacity: DEFAULT_WRITE_CHANNEL_CAPACITY,
            slow_write_warn_ms: DEFAULT_SLOW_WRITE_WARN_MS,
            slow_read_warn_ms: DEFAULT_SLOW_READ_WARN_MS,
        }
    }

    /// Create a new in-process executor from the application config.
    ///
    /// Loads `python.data_source_write_channel_capacity`, `python.data_source_slow_write_warn_ms`,
    /// and `python.data_source_slow_read_warn_ms` from the application config.
    /// Falls back to defaults if config loading fails.
    pub fn from_app_config() -> Self {
        Self::try_from_app_config().unwrap_or_else(|_| Self::new())
    }

    /// Create a new in-process executor from the application config.
    ///
    /// Returns an error if config loading fails.
    pub fn try_from_app_config() -> Result<Self, sail_common::error::CommonError> {
        let config = sail_common::config::AppConfig::load()?;
        Ok(Self {
            write_channel_capacity: config.python.data_source_write_channel_capacity,
            slow_write_warn_ms: config.python.data_source_slow_write_warn_ms,
            slow_read_warn_ms: config.python.data_source_slow_read_warn_ms,
        })
    }

    /// Create a new in-process executor with the given config values.
    pub fn with_config(
        write_channel_capacity: usize,
        slow_write_warn_ms: u64,
        slow_read_warn_ms: u64,
    ) -> Self {
        Self {
            write_channel_capacity,
            slow_write_warn_ms,
            slow_read_warn_ms,
        }
    }
}

impl Default for InProcessExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PythonExecutor for InProcessExecutor {
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef> {
        let command = command.to_vec();

        // Use spawn_blocking for GIL-bound operations
        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                // Deserialize and call schema()
                let datasource = deserialize_datasource(py, &command)?;

                // Get datasource name for error context
                let ds_name = datasource
                    .call_method0("name")
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let ctx = PythonDataSourceContext::new(&ds_name, "schema");

                let schema_obj = datasource
                    .call_method0("schema")
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Convert to Arrow schema
                super::arrow_utils::py_schema_to_rust(py, &schema_obj).map_err(|e| {
                    datafusion_common::DataFusionError::External(Box::new(
                        ctx.wrap_error(format!("Failed to convert schema: {}", e)),
                    ))
                })
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn get_partitions(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        filters: Vec<PythonFilter>,
    ) -> Result<PartitionPlan> {
        let command = command.to_vec();
        let schema = schema.clone();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                let datasource = deserialize_datasource(py, &command)?;

                // Get datasource name for error context
                let ds_name = datasource
                    .call_method0("name")
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let ctx = PythonDataSourceContext::new(&ds_name, "partitions");

                // Get reader with schema (PySpark 4.x API requires schema argument)
                let schema_obj = super::arrow_utils::rust_schema_to_py(py, &schema)?;
                let reader = datasource
                    .call_method1("reader", (schema_obj,))
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Push filters to reader if any were provided
                if !filters.is_empty() {
                    let filter_ctx = PythonDataSourceContext::new(&ds_name, "pushFilters");

                    // Convert Rust filters to Python filter objects
                    let py_filters = filters_to_python(py, &filters).map_err(|e| {
                        filter_ctx.wrap_error(format!("Failed to convert filters: {}", e))
                    })?;

                    // Call pushFilters on the reader
                    let rejected = reader
                        .call_method1("pushFilters", (py_filters,))
                        .map_err(|e| filter_ctx.wrap_py_error(e))?;

                    // Count rejected filters for logging
                    use pyo3::types::PyIterator;
                    let rejected_list = PyIterator::from_object(&rejected).map_err(|e| {
                        filter_ctx.wrap_error(format!("pushFilters must return an iterator: {}", e))
                    })?;
                    let rejected_count = rejected_list.count();

                    log::debug!(
                        "[{}::pushFilters] Pushed {} filters, {} rejected",
                        ds_name,
                        filters.len(),
                        rejected_count
                    );
                }

                // Now call partitions() on the same reader that has the filters
                let partitions = reader
                    .call_method0("partitions")
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Convert Python partitions to Rust
                let partitions_list =
                    partitions.downcast::<pyo3::types::PyList>().map_err(|e| {
                        ctx.wrap_error(format!("partitions() must return a list: {}", e))
                    })?;

                let mut result = Vec::with_capacity(partitions_list.len());
                let mut total_size: usize = 0;

                for (i, partition) in partitions_list.iter().enumerate() {
                    // Pickle each partition for distribution
                    let pickled = pickle_object(py, &partition)?;
                    let partition_size = pickled.len();

                    total_size += partition_size;
                    result.push(InputPartition {
                        partition_id: i,
                        data: pickled,
                    });
                }

                // Pickle the reader with filters already applied
                let pickled_reader = pickle_object(py, &reader)?;

                log::debug!(
                    "[{}::partitions] Created {} partitions, total size: {} bytes, reader size: {} bytes",
                    ds_name,
                    result.len(),
                    total_size,
                    pickled_reader.len()
                );

                Ok(PartitionPlan {
                    pickled_reader,
                    partitions: result,
                })
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn execute_read(
        &self,
        pickled_reader: &[u8],
        partition: &InputPartition,
        schema: SchemaRef,
        batch_size: usize,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        use super::stream::PythonDataSourceStream;

        let stream = PythonDataSourceStream::new(
            pickled_reader.to_vec(),
            partition.clone(),
            schema,
            batch_size,
            self.slow_read_warn_ms,
        )?;

        Ok(Box::pin(stream))
    }

    async fn get_writer(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        overwrite: bool,
    ) -> Result<WriterPlan> {
        let command = command.to_vec();
        let schema = schema.clone();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                let datasource = deserialize_datasource(py, &command)?;

                // Get datasource name for error context
                let ds_name = datasource
                    .call_method0("name")
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let ctx = PythonDataSourceContext::new(&ds_name, "writer");

                // Get writer with schema and overwrite mode
                let schema_obj = super::arrow_utils::rust_schema_to_py(py, &schema)?;
                let writer = datasource
                    .call_method1("writer", (schema_obj, overwrite))
                    .map_err(|e| ctx.wrap_py_error(e))?;

                // Check if it's an Arrow-based writer or Row-based writer
                // Import pyspark.sql.datasource module
                let pyspark_ds = py.import("pyspark.sql.datasource").map_err(|e| {
                    ctx.wrap_error(format!("Failed to import pyspark.sql.datasource: {}", e))
                })?;

                let arrow_writer_class =
                    pyspark_ds.getattr("DataSourceArrowWriter").map_err(|e| {
                        ctx.wrap_error(format!("Failed to get DataSourceArrowWriter class: {}", e))
                    })?;

                let is_arrow = writer
                    .is_instance(&arrow_writer_class)
                    .map_err(|e| ctx.wrap_error(format!("Failed to check isinstance: {}", e)))?;

                // Pickle the writer
                let pickled_writer = pickle_object(py, &writer)?;

                log::debug!(
                    "[{}::writer] Created {} writer, size: {} bytes",
                    ds_name,
                    if is_arrow { "Arrow" } else { "Row" },
                    pickled_writer.len()
                );

                Ok(WriterPlan {
                    pickled_writer,
                    is_arrow,
                })
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn execute_write(
        &self,
        pickled_writer: &[u8],
        schema: SchemaRef,
        is_arrow: bool,
        mut batches: BoxStream<'static, Result<RecordBatch>>,
    ) -> Result<WriteResult> {
        let pickled_writer = pickled_writer.to_vec();

        // Create a channel to stream batches to the blocking thread.
        // Capacity is from python.data_source_write_channel_capacity config; larger values
        // trade memory for throughput when the writer is slower than the producer.
        let (tx, rx) = std::sync::mpsc::sync_channel(self.write_channel_capacity);

        // Cancellation flag: set when the blocking write task finishes so the
        // pumper stops immediately instead of draining the entire input stream.
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_pumper = cancel.clone();

        // Spawn a task to pump batches into the channel
        let pumper = tokio::task::spawn(async move {
            use futures::StreamExt;
            while let Some(batch_result) = batches.next().await {
                if cancel_pumper.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                if tx.send(batch_result).is_err() {
                    break;
                }
            }
        });

        let slow_write_warn_ms = self.slow_write_warn_ms;

        let result = tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                // Deserialize the writer
                let writer = deserialize_object(py, &pickled_writer)?;

                // Get writer name for error context
                let writer_name = writer
                    .getattr("__class__")
                    .and_then(|c| c.getattr("__name__"))
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let ctx = PythonDataSourceContext::new(&writer_name, "write");

                // Create the streaming iterator
                let py_iter = Py::new(
                    py,
                    RecordBatchIterator::new(rx, is_arrow, schema.clone(), py)?,
                )
                .map_err(py_err)?;

                // Call writer.write(iterator) with timing
                let write_start = std::time::Instant::now();
                let commit_msg = writer
                    .call_method1("write", (py_iter,))
                    .map_err(|e| ctx.wrap_py_error(e))?;
                let write_elapsed_ms = write_start.elapsed().as_millis() as u64;

                if write_elapsed_ms > slow_write_warn_ms {
                    log::warn!(
                        "[{}::write] Slow write detected: {}ms (threshold: {}ms). \
                        Consider using faster I/O or subprocess isolation.",
                        writer_name,
                        write_elapsed_ms,
                        slow_write_warn_ms
                    );
                }

                // Pickle the commit message if not None
                let commit_message = if commit_msg.is_none() {
                    None
                } else {
                    Some(pickle_object(py, &commit_msg)?)
                };

                log::debug!(
                    "[{}::write] Completed write in {}ms, commit message: {} bytes",
                    writer_name,
                    write_elapsed_ms,
                    commit_message.as_ref().map(|m| m.len()).unwrap_or(0)
                );

                Ok(WriteResult { commit_message })
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Signal pumper to stop (it may still be reading upstream batches)
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);

        // Ensure the pumper task also finished
        let _ = pumper.await;

        result
    }

    async fn commit_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()> {
        let pickled_writer = pickled_writer.to_vec();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                // Deserialize the writer
                let writer = deserialize_object(py, &pickled_writer)?;

                // Get writer name for error context
                let writer_name = writer
                    .getattr("__class__")
                    .and_then(|c| c.getattr("__name__"))
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                let ctx = PythonDataSourceContext::new(&writer_name, "commit");

                // Unpickle commit messages, using Python None for missing messages
                let py_messages: Vec<Py<PyAny>> = commit_messages
                    .iter()
                    .map(|msg| match msg {
                        Some(bytes) => deserialize_object(py, bytes).map(|obj| obj.unbind()),
                        None => Ok(py.None()),
                    })
                    .collect::<Result<_>>()?;

                // Create Python list of messages
                let messages_list = pyo3::types::PyList::new(py, py_messages).map_err(py_err)?;

                // Call writer.commit(messages)
                writer
                    .call_method1("commit", (messages_list,))
                    .map_err(|e| ctx.wrap_py_error(e))?;

                log::debug!(
                    "[{}::commit] Committed {} partitions",
                    writer_name,
                    commit_messages.len()
                );

                Ok(())
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn abort_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()> {
        let pickled_writer = pickled_writer.to_vec();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                // Deserialize the writer (best-effort: abort must not propagate errors
                // since the original error from write/commit is more important)
                let writer = match deserialize_object(py, &pickled_writer) {
                    Ok(w) => w,
                    Err(e) => {
                        log::error!(
                            "Failed to deserialize writer for abort (best-effort, swallowed): {}",
                            e
                        );
                        return Ok(());
                    }
                };

                // Get writer name for logging
                let writer_name = writer
                    .getattr("__class__")
                    .and_then(|c| c.getattr("__name__"))
                    .and_then(|n| n.extract::<String>())
                    .unwrap_or_else(|_| "<unknown>".to_string());

                // Unpickle commit messages (best effort), using Python None for missing
                let py_messages: Vec<Py<PyAny>> = commit_messages
                    .iter()
                    .map(|msg| match msg {
                        Some(bytes) => deserialize_object(py, bytes)
                            .map(|obj| obj.unbind())
                            .unwrap_or_else(|_| py.None()),
                        None => py.None(),
                    })
                    .collect();

                // Create Python list of messages
                let messages_list = match pyo3::types::PyList::new(py, py_messages) {
                    Ok(list) => list,
                    Err(e) => {
                        log::warn!(
                            "[{}::abort] Failed to create messages list: {}",
                            writer_name,
                            e
                        );
                        return Ok(());
                    }
                };

                // Call writer.abort(messages) - log errors but don't propagate
                if let Err(e) = writer.call_method1("abort", (messages_list,)) {
                    log::warn!("[{}::abort] Abort call failed: {}", writer_name, e);
                } else {
                    log::debug!(
                        "[{}::abort] Aborted {} partitions",
                        writer_name,
                        commit_messages.len()
                    );
                }

                Ok(())
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }
}

/// A Python iterator that yields data from Arrow RecordBatches.
///
/// This enables streaming data from Rust to Python without loading
/// everything into memory at once.
#[pyclass]
pub struct RecordBatchIterator {
    /// Wrapped in Mutex so the struct is Sync (required by PyO3 for pyclass).
    receiver: std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<Result<RecordBatch>>>>,
    is_arrow: bool,
    row_factory: Option<Py<PyAny>>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
}

impl RecordBatchIterator {
    /// Create a new RecordBatchIterator.
    pub fn new(
        receiver: std::sync::mpsc::Receiver<Result<RecordBatch>>,
        is_arrow: bool,
        schema: SchemaRef,
        py: Python<'_>,
    ) -> Result<Self> {
        let row_factory = if is_arrow {
            None
        } else {
            Some(super::arrow_utils::get_row_factory(py, &schema)?)
        };

        Ok(Self {
            receiver: std::sync::Arc::new(std::sync::Mutex::new(receiver)),
            is_arrow,
            row_factory,
            current_batch: None,
            current_row: 0,
        })
    }
}

#[pymethods]
impl RecordBatchIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();

        loop {
            // If we have an active batch, try to get the next item
            if let Some(batch) = &slf.current_batch {
                if slf.current_row < batch.num_rows() {
                    // Compute result and next row index without mutating slf while borrowing batch
                    let (result, next_row) = if slf.is_arrow {
                        // For arrow, return one batch at a time (DataSourceArrowWriter.write(iterator))
                        let r = super::arrow_utils::rust_record_batch_to_py(py, batch).map_err(
                            |e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()),
                        )?;
                        (r, batch.num_rows())
                    } else {
                        // For row, return one row at a time
                        let row_idx = slf.current_row;
                        let factory = slf
                            .row_factory
                            .as_ref()
                            .ok_or_else(|| {
                                pyo3::exceptions::PyRuntimeError::new_err(
                                    "row_factory is None for row-based writer",
                                )
                            })?
                            .bind(py);
                        let mut row_values = Vec::with_capacity(batch.num_columns());
                        for col_idx in 0..batch.num_columns() {
                            let column = batch.column(col_idx);
                            let value =
                                super::arrow_utils::extract_python_value(py, column, row_idx)
                                    .map_err(|e| {
                                        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                                    })?;
                            row_values.push(value);
                        }
                        let args = pyo3::types::PyTuple::new(py, row_values).map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                        })?;
                        let r = factory
                            .call1(args)
                            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
                            .unbind();
                        (r, row_idx + 1)
                    };
                    slf.current_row = next_row;
                    return Ok(Some(result));
                }
            }

            // No active batch or batch exhausted, get the next one from the channel
            // Release the GIL while waiting for the next batch from Rust.
            let receiver = std::sync::Arc::clone(&slf.receiver);
            let batch_result = py.detach(|| -> PyResult<_> {
                let guard = receiver
                    .lock()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(guard.recv())
            })?;
            match batch_result {
                Ok(Ok(batch)) => {
                    slf.current_batch = Some(batch);
                    slf.current_row = 0;
                    // Loop around to extract from the new batch
                }
                Ok(Err(e)) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()));
                }
                Err(_) => {
                    // Channel closed
                    return Ok(None);
                }
            }
        }
    }
}

/// Deserialize a pickled Python datasource.
fn deserialize_datasource<'py>(
    py: pyo3::Python<'py>,
    command: &[u8],
) -> Result<pyo3::Bound<'py, pyo3::PyAny>> {
    use pyo3::types::PyBytes;

    let cloudpickle = import_cloudpickle(py)?;
    let py_bytes = PyBytes::new(py, command);

    cloudpickle
        .call_method1("loads", (py_bytes,))
        .map_err(py_err)
}

/// Deserialize a pickled Python object (generic version).
fn deserialize_object<'py>(
    py: pyo3::Python<'py>,
    pickled: &[u8],
) -> Result<pyo3::Bound<'py, pyo3::PyAny>> {
    use pyo3::types::PyBytes;

    let cloudpickle = import_cloudpickle(py)?;
    let py_bytes = PyBytes::new(py, pickled);

    cloudpickle
        .call_method1("loads", (py_bytes,))
        .map_err(py_err)
}

/// Pickle a Python object for distribution.
fn pickle_object(py: pyo3::Python<'_>, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Vec<u8>> {
    let cloudpickle = import_cloudpickle(py)?;
    let pickled = cloudpickle.call_method1("dumps", (obj,)).map_err(py_err)?;
    let bytes = pickled
        .extract::<Vec<u8>>()
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
    Ok(bytes)
}

/// Re-export py_err from error module.
use super::error::py_err;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_partition_clone() {
        let partition = InputPartition {
            partition_id: 0,
            data: vec![1, 2, 3],
        };
        let cloned = partition.clone();
        assert_eq!(cloned.partition_id, 0);
        assert_eq!(cloned.data, vec![1, 2, 3]);
    }
}
