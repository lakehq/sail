use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{exec_err, DataFusionError, Result};
use futures::{Stream, StreamExt};
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::PyAnyMethods;
use pyo3::{pyclass, pymethods, IntoPyObject, PyObject, PyRef, PyRefMut, PyResult, Python};
use sail_common_datafusion::utils::record_batch_with_schema;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::error::PyUdfResult;

struct PyInputStreamState {
    input: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl PyInputStreamState {
    async fn next(&mut self) -> Option<Result<RecordBatch>> {
        select! {
            x = self.input.next() => x,
            _ = &mut self.signal => Some(exec_err!("stop signal received for the Python input stream")),
        }
    }
}

#[pyclass]
struct PyInputStream {
    state: Arc<tokio::sync::Mutex<PyInputStreamState>>,
    handle: Handle,
}

impl PyInputStream {
    pub fn new(
        input: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        handle: Handle,
    ) -> Self {
        Self {
            state: Arc::new(tokio::sync::Mutex::new(PyInputStreamState {
                input,
                signal,
            })),
            handle,
        }
    }
}

#[pymethods]
impl PyInputStream {
    fn __iter__(self_: PyRef<Self>) -> PyRef<Self> {
        self_
    }

    fn __next__(self_: PyRefMut<'_, Self>) -> PyResult<PyObject> {
        let state = Arc::clone(&self_.state);
        let handle = self_.handle.clone();
        self_
            .py()
            .allow_threads(|| {
                handle.block_on(async {
                    state
                        .lock()
                        .await
                        .next()
                        .await
                        .map(|x| x.map_err(|e| PyRuntimeError::new_err(e.to_string())))
                })
            })
            .map(|x| x.and_then(|x| x.to_pyarrow(self_.py())))
            .unwrap_or(Err(PyStopIteration::new_err("")))
    }
}

enum PyMapStreamState {
    Running {
        signal: oneshot::Sender<()>,
        python_task: std::thread::JoinHandle<()>,
    },
    Stopped,
}

pub struct PyMapStream {
    inner: SendableRecordBatchStream,
    state: PyMapStreamState,
}

impl PyMapStream {
    const OUTPUT_CHANNEL_BUFFER: usize = 16;

    pub fn new(
        input: SendableRecordBatchStream,
        function: PyObject,
        output_schema: SchemaRef,
    ) -> Self {
        let (output_tx, output_rx) = mpsc::channel(Self::OUTPUT_CHANNEL_BUFFER);
        let (signal_tx, signal_rx) = oneshot::channel();
        let handle = Handle::current();
        let python_output_schema = output_schema.clone();
        // We have to spawn a thread instead of spawning a tokio task
        // due to the blocking operation inside the input iterator.
        let python_task = std::thread::spawn(move || {
            match Python::with_gil(|py| {
                Self::run_python_task(
                    py,
                    function,
                    input,
                    python_output_schema,
                    signal_rx,
                    output_tx.clone(),
                    handle,
                )
            }) {
                Ok(()) => {}
                Err(e) => {
                    let _ = output_tx.blocking_send(Err(e.into()));
                }
            }
        });
        Self {
            inner: Box::pin(RecordBatchStreamAdapter::new(
                output_schema,
                ReceiverStream::new(output_rx),
            )),
            state: PyMapStreamState::Running {
                signal: signal_tx,
                python_task,
            },
        }
    }

    fn run_python_task(
        py: Python,
        function: PyObject,
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
        signal: oneshot::Receiver<()>,
        sender: mpsc::Sender<Result<RecordBatch>>,
        handle: Handle,
    ) -> PyUdfResult<()> {
        // Create a Python iterator from the input record batch stream and call the function.
        // We could have wrap each record batch in a single-element list and call the function
        // for each record batch, but that does not work if the user wants to maintain state
        // across record batches.
        let input = PyInputStream::new(input, signal, handle);
        let input = input.into_pyobject(py)?;
        let output = function.call1(py, (input,))?.into_bound(py);
        for batch in output.try_iter()? {
            // Ignore empty record batches since the PySpark unit tests expect them to be ignored
            // even if they have incompatible schemas.
            if batch.as_ref().is_ok_and(|x| x.is_empty().unwrap_or(false)) {
                continue;
            }
            let batch = batch.and_then(|x| RecordBatch::from_pyarrow_bound(&x));
            if py
                .allow_threads(|| {
                    let batch = batch
                        .map_err(|e| DataFusionError::External(e.into()))
                        .and_then(|x| record_batch_with_schema(x, &output_schema));
                    sender.blocking_send(batch)
                })
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }
}

impl Drop for PyMapStream {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, PyMapStreamState::Stopped);
        match state {
            PyMapStreamState::Running {
                signal,
                python_task,
            } => {
                let _ = signal.send(());
                // This may block indefinitely waiting for the Python function
                // to finish processing all input batches received before the stop signal.
                // Unfortunately, there is no reliable way to abort the thread cleanly,
                // so we have to wait for it to finish.
                let _ = python_task.join();
            }
            PyMapStreamState::Stopped => {}
        }
    }
}

impl Stream for PyMapStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for PyMapStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
