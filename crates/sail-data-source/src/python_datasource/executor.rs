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
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyAnyMethods;

/// Input partition for parallel reading.
#[derive(Debug, Clone)]
pub struct InputPartition {
    /// Partition identifier
    pub partition_id: usize,
    /// Pickled partition data (opaque to Rust)
    pub data: Vec<u8>,
}

/// Abstract executor for Python datasource operations.
///
/// This trait provides the abstraction layer between Sail's execution engine
/// and Python datasource implementations. The MVP uses `InProcessExecutor`,
/// but the trait is designed for future `RemoteExecutor` implementation.
#[async_trait]
pub trait PythonExecutor: Send + Sync {
    /// Get the schema from the Python datasource.
    ///
    /// Calls the Python `DataSource.schema()` method.
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef>;

    /// Get partitions for parallel reading.
    ///
    /// Calls the Python `DataSource.reader().partitions()` method.
    async fn get_partitions(&self, command: &[u8]) -> Result<Vec<InputPartition>>;

    /// Execute a read for a specific partition.
    ///
    /// Returns a stream of RecordBatches from Python.
    async fn execute_read(
        &self,
        command: &[u8],
        partition: &InputPartition,
        schema: SchemaRef,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// In-process executor using PyO3.
///
/// This is the MVP implementation that calls Python directly via PyO3.
/// GIL contention is acceptable for control plane operations (schema, partitions).
/// Data plane uses Arrow C Data Interface for efficiency.
#[cfg(feature = "python")]
pub struct InProcessExecutor {
    /// Python version for compatibility checking
    #[allow(dead_code)]
    python_ver: String,
}

#[cfg(feature = "python")]
impl InProcessExecutor {
    /// Create a new in-process executor.
    pub fn new(python_ver: String) -> Self {
        Self { python_ver }
    }
}

#[cfg(feature = "python")]
#[async_trait]
impl PythonExecutor for InProcessExecutor {
    async fn get_schema(&self, command: &[u8]) -> Result<SchemaRef> {
        let command = command.to_vec();

        // Use spawn_blocking for GIL-bound operations
        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                // Deserialize and call schema()
                let datasource = deserialize_datasource(py, &command)?;
                let schema_obj = datasource.call_method0("schema").map_err(py_err)?;

                // Convert to Arrow schema
                super::arrow_utils::py_schema_to_rust(py, &schema_obj)
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn get_partitions(&self, command: &[u8]) -> Result<Vec<InputPartition>> {
        let command = command.to_vec();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::attach(|py| {
                let datasource = deserialize_datasource(py, &command)?;

                // Get reader and partitions
                let reader = datasource.call_method0("reader").map_err(py_err)?;
                let partitions = reader.call_method0("partitions").map_err(py_err)?;

                // Convert Python partitions to Rust
                let partitions_list = partitions
                    .downcast::<pyo3::types::PyList>()
                    .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;

                let mut result = Vec::with_capacity(partitions_list.len());
                for (i, partition) in partitions_list.iter().enumerate() {
                    // Pickle each partition for distribution
                    let pickled = pickle_object(py, &partition)?;
                    result.push(InputPartition {
                        partition_id: i,
                        data: pickled,
                    });
                }

                Ok(result)
            })
        })
        .await
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
    }

    async fn execute_read(
        &self,
        command: &[u8],
        partition: &InputPartition,
        schema: SchemaRef,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        use super::stream::PythonDataSourceStream;

        let stream = PythonDataSourceStream::new(command.to_vec(), partition.clone(), schema)?;

        Ok(Box::pin(stream))
    }
}

/// Deserialize a pickled Python datasource.
#[cfg(feature = "python")]
fn deserialize_datasource<'py>(
    py: pyo3::Python<'py>,
    command: &[u8],
) -> Result<pyo3::Bound<'py, pyo3::PyAny>> {
    use pyo3::types::PyBytes;

    let cloudpickle = py.import("cloudpickle").map_err(py_err)?;
    let py_bytes = PyBytes::new(py, command);

    cloudpickle
        .call_method1("loads", (py_bytes,))
        .map_err(py_err)
}

/// Pickle a Python object for distribution.
#[cfg(feature = "python")]
fn pickle_object(py: pyo3::Python<'_>, obj: &pyo3::Bound<'_, pyo3::PyAny>) -> Result<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle").map_err(py_err)?;
    let pickled = cloudpickle.call_method1("dumps", (obj,)).map_err(py_err)?;
    let bytes = pickled
        .extract::<Vec<u8>>()
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
    Ok(bytes)
}

/// Convert PyO3 error to DataFusion error.
#[cfg(feature = "python")]
fn py_err(e: pyo3::PyErr) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
}

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
