use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::{DataFusionError, Result};
use futures::Stream;
use tokio::sync::{mpsc, oneshot};

/// RecordBatch stream from Python DataSource.
///
/// This stream reads from a Python datasource in a dedicated thread,
/// with proper RAII cleanup via the Drop impl.
///
/// Key patterns (from sail_engineering skill):
/// - std::thread::spawn for Python (GIL constraints)
/// - oneshot signal for graceful shutdown
/// - Thread join in Drop to prevent leaks
use super::executor::InputPartition;

/// Default batch size for collecting rows.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// Stream state for RAII cleanup.
enum StreamState {
    Running {
        /// Signal to stop the Python thread
        stop_signal: Option<oneshot::Sender<()>>,
        /// Handle to join the Python thread
        python_thread: Option<std::thread::JoinHandle<()>>,
        /// Receiver for batches
        rx: mpsc::Receiver<Result<RecordBatch>>,
    },
    Stopped,
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
    #[cfg(feature = "python")]
    pub fn new(command: Vec<u8>, partition: InputPartition, schema: SchemaRef) -> Result<Self> {
        let (tx, rx) = mpsc::channel(16);
        let (stop_tx, stop_rx) = oneshot::channel();

        let schema_clone = schema.clone();

        // Spawn Python thread
        let python_thread = std::thread::spawn(move || {
            Self::run_python_reader(command, partition, schema_clone, tx, stop_rx);
        });

        Ok(Self {
            schema,
            state: StreamState::Running {
                stop_signal: Some(stop_tx),
                python_thread: Some(python_thread),
                rx,
            },
        })
    }

    /// Run the Python reader in a dedicated thread.
    #[cfg(feature = "python")]
    fn run_python_reader(
        command: Vec<u8>,
        partition: InputPartition,
        schema: SchemaRef,
        tx: mpsc::Sender<Result<RecordBatch>>,
        mut stop_rx: oneshot::Receiver<()>,
    ) {
        use pyo3::prelude::*;
        use pyo3::types::PyBytes;

        let result = Python::attach(|py| -> Result<()> {
            // Deserialize datasource
            let cloudpickle = py.import("cloudpickle").map_err(py_err)?;
            let command_bytes = PyBytes::new(py, &command);
            let datasource = cloudpickle
                .call_method1("loads", (command_bytes,))
                .map_err(py_err)?;

            // Deserialize partition
            let partition_bytes = PyBytes::new(py, &partition.data);
            let py_partition = cloudpickle
                .call_method1("loads", (partition_bytes,))
                .map_err(py_err)?;

            // Get reader and call read()
            let schema_obj = super::arrow_utils::rust_schema_to_py(py, &schema)?;
            let reader = datasource
                .call_method1("reader", (schema_obj,))
                .map_err(py_err)?;
            let iterator = reader
                .call_method1("read", (py_partition,))
                .map_err(py_err)?;

            // Iterate over results
            loop {
                // Check for stop signal
                match stop_rx.try_recv() {
                    Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                        break;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                // Get next item from iterator
                match iterator.call_method0("__next__") {
                    Ok(item) => {
                        // Convert to RecordBatch
                        let batch = super::arrow_utils::py_record_batch_to_rust(py, &item)?;

                        // Send batch
                        if tx.blocking_send(Ok(batch)).is_err() {
                            // Receiver dropped, stop
                            break;
                        }
                    }
                    Err(e) => {
                        // Check if StopIteration (normal end)
                        if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                            break;
                        }
                        // Other error
                        let _ = tx.blocking_send(Err(py_err(e)));
                        break;
                    }
                }
            }

            Ok(())
        });

        // Send any error
        if let Err(e) = result {
            let _ = tx.blocking_send(Err(e));
        }
    }

    /// Create a placeholder stream (for non-Python builds).
    #[cfg(not(feature = "python"))]
    pub fn new(_command: Vec<u8>, _partition: InputPartition, schema: SchemaRef) -> Result<Self> {
        Err(DataFusionError::NotImplemented(
            "Python support not enabled".to_string(),
        ))
    }
}

impl Stream for PythonDataSourceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            StreamState::Running { rx, .. } => Pin::new(rx).poll_recv(cx),
            StreamState::Stopped => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for PythonDataSourceStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for PythonDataSourceStream {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, StreamState::Stopped);

        match state {
            StreamState::Running {
                stop_signal,
                python_thread,
                ..
            } => {
                // Send stop signal
                if let Some(signal) = stop_signal {
                    let _ = signal.send(());
                }

                // Join thread to ensure cleanup
                if let Some(thread) = python_thread {
                    // Don't panic if thread panicked
                    let _ = thread.join();
                }
            }
            StreamState::Stopped => {}
        }
    }
}

/// Convert PyO3 error to DataFusion error.
#[cfg(feature = "python")]
fn py_err(e: pyo3::PyErr) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(e.to_string())))
}

/// Helper for collecting rows into batches.
pub struct RowBatchCollector {
    /// Schema for the batch
    schema: SchemaRef,
    /// Collected rows (as Python tuples, pickled)
    rows: Vec<Vec<u8>>,
    /// Batch size threshold
    batch_size: usize,
}

impl RowBatchCollector {
    /// Create a new collector.
    pub fn new(schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            schema,
            rows: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    /// Add a row (pickled tuple).
    pub fn add_row(&mut self, row: Vec<u8>) {
        self.rows.push(row);
    }

    /// Check if batch is ready.
    pub fn is_ready(&self) -> bool {
        self.rows.len() >= self.batch_size
    }

    /// Flush collected rows to a batch.
    #[cfg(feature = "python")]
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let rows = std::mem::take(&mut self.rows);
        let batch = super::arrow_utils::convert_rows_to_batch(&self.schema, &rows)?;
        Ok(Some(batch))
    }

    #[cfg(not(feature = "python"))]
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        Err(DataFusionError::NotImplemented(
            "Python support not enabled".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_row_batch_collector() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let mut collector = RowBatchCollector::new(schema, 100);

        assert!(!collector.is_ready());

        // Add rows
        for _ in 0..50 {
            collector.add_row(vec![1, 2, 3]);
        }

        assert!(!collector.is_ready());

        // Add more to reach threshold
        for _ in 0..60 {
            collector.add_row(vec![1, 2, 3]);
        }

        assert!(collector.is_ready());
    }
}
