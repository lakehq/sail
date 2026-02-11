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
    /// Pickled Python data sourceReader instance (with filters applied)
    pub pickled_reader: Vec<u8>,
    /// Partitions for parallel reading
    pub partitions: Vec<InputPartition>,
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
}

/// In-process executor using PyO3.
///
/// This is the MVP implementation that calls Python directly via PyO3.
/// GIL contention is acceptable for control plane operations (schema, partitions).
/// Data plane uses Arrow C Data Interface for efficiency.
///
/// Note: Python version validation happens in PythonDataSource::new(), not here.
#[derive(Debug, Default)]
pub struct InProcessExecutor;

impl InProcessExecutor {
    /// Create a new in-process executor.
    pub fn new() -> Self {
        Self
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
        )?;

        Ok(Box::pin(stream))
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
