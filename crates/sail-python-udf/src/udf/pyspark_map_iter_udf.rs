use std::cmp::Ordering;
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
use pyo3::types::PyList;
use pyo3::{pyclass, pymethods, Bound, IntoPy, PyAny, PyObject, PyRef, PyRefMut, PyResult, Python};
use sail_common::udf::MapIterUDF;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::error::PyUdfResult;
use crate::utils::pandas::PandasDataFrame;
use crate::utils::pyarrow::{PyArrowRecordBatch, PyArrowToPandasOptions};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkMapIterUDF {
    format: PySparkMapIterFormat,
    function_name: String,
    function: Vec<u8>,
    output_schema: SchemaRef,
}

impl PySparkMapIterUDF {
    pub fn new(
        format: PySparkMapIterFormat,
        function_name: String,
        function: Vec<u8>,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            format,
            function_name,
            function,
            output_schema,
        }
    }

    pub fn format(&self) -> PySparkMapIterFormat {
        self.format
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn function(&self) -> &[u8] {
        &self.function
    }
}

#[derive(PartialEq, PartialOrd)]
struct PySparkMapIterUDFOrd<'a> {
    format: PySparkMapIterFormat,
    function_name: &'a String,
    function: &'a Vec<u8>,
}

impl<'a> From<&'a PySparkMapIterUDF> for PySparkMapIterUDFOrd<'a> {
    fn from(udf: &'a PySparkMapIterUDF) -> Self {
        Self {
            format: udf.format,
            function_name: &udf.function_name,
            function: &udf.function,
        }
    }
}

impl PartialOrd for PySparkMapIterUDF {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PySparkMapIterUDFOrd::from(self).partial_cmp(&other.into())
    }
}

impl MapIterUDF for PySparkMapIterUDF {
    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let function = Python::with_gil(|py| PySparkUdfObject::load(py, &self.function))?;
        Ok(Box::pin(MapIterStream::new(
            input,
            function,
            self.format,
            self.output_schema.clone(),
        )))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum PySparkMapIterFormat {
    Pandas,
    Arrow,
}

impl PySparkMapIterFormat {
    fn record_batch_to_py(&self, py: Python, batch: RecordBatch) -> PyResult<PyObject> {
        let batch = batch.to_pyarrow(py)?;
        match self {
            PySparkMapIterFormat::Pandas => Self::pyarrow_to_pandas(py, batch),
            PySparkMapIterFormat::Arrow => Ok(batch),
        }
    }

    fn record_batch_from_py_bound(
        &self,
        batch: Bound<PyAny>,
        schema: &SchemaRef,
    ) -> PyResult<RecordBatch> {
        let batch = match self {
            PySparkMapIterFormat::Pandas => Self::pyarrow_from_pandas_bound(batch, schema)?,
            PySparkMapIterFormat::Arrow => batch,
        };
        RecordBatch::from_pyarrow_bound(&batch)
    }

    fn pyarrow_to_pandas(py: Python, batch: PyObject) -> PyResult<PyObject> {
        let converter = PyArrowRecordBatch::to_pandas(
            py,
            PyArrowToPandasOptions {
                // The PySpark unit tests do not expect Pandas nullable types.
                use_pandas_nullable_types: false,
            },
        )?;
        Ok(converter.call1((batch,))?.into())
    }

    fn pyarrow_from_pandas_bound<'py>(
        df: Bound<'py, PyAny>,
        schema: &SchemaRef,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = df.py();
        let df = if PandasDataFrame::has_string_columns(&df)? {
            df
        } else {
            PandasDataFrame::rename_columns_by_position(&df, schema)?
        };
        let converter = PyArrowRecordBatch::from_pandas(py, Some(schema.to_pyarrow(py)?))?;
        converter.call1((df,))
    }
}

struct PyMapIterInputStreamState {
    input: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
}

impl PyMapIterInputStreamState {
    async fn next(&mut self) -> Option<Result<RecordBatch>> {
        select! {
            x = self.input.next() => x,
            _ = &mut self.signal => Some(exec_err!("stop signal received for the Python map iterator")),
        }
    }
}

#[pyclass]
struct PyMapIterInputStream {
    format: PySparkMapIterFormat,
    state: Arc<tokio::sync::Mutex<PyMapIterInputStreamState>>,
    handle: Handle,
}

impl PyMapIterInputStream {
    pub fn new(
        format: PySparkMapIterFormat,
        input: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        handle: Handle,
    ) -> Self {
        Self {
            format,
            state: Arc::new(tokio::sync::Mutex::new(PyMapIterInputStreamState {
                input,
                signal,
            })),
            handle,
        }
    }
}

#[pymethods]
impl PyMapIterInputStream {
    fn __iter__(self_: PyRef<Self>) -> PyRef<Self> {
        self_
    }

    fn __next__(self_: PyRefMut<'_, Self>) -> PyResult<PyObject> {
        let state = Arc::clone(&self_.state);
        let handle = self_.handle.clone();
        let format = self_.format;
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
            .map(|x| {
                let batch = x.and_then(|x| format.record_batch_to_py(self_.py(), x))?;
                let batch = PyList::new_bound(self_.py(), vec![batch]);
                Ok(batch.into())
            })
            .unwrap_or(Err(PyStopIteration::new_err("")))
    }
}

enum MapIterStreamState {
    Running {
        signal: oneshot::Sender<()>,
        python_task: std::thread::JoinHandle<()>,
    },
    Stopped,
}

struct MapIterStream {
    inner: SendableRecordBatchStream,
    state: MapIterStreamState,
}

impl MapIterStream {
    const OUTPUT_CHANNEL_BUFFER: usize = 16;

    pub fn new(
        input: SendableRecordBatchStream,
        function: PyObject,
        format: PySparkMapIterFormat,
        output_schema: SchemaRef,
    ) -> Self {
        let (output_tx, output_rx) = mpsc::channel(Self::OUTPUT_CHANNEL_BUFFER);
        let (signal_tx, signal_rx) = oneshot::channel();
        let handle = Handle::current();
        let output_schema_for_python = output_schema.clone();
        // We have to spawn a thread instead of spawning a tokio task
        // due to the blocking operation inside the input iterator.
        let python_task = std::thread::spawn(move || {
            match Python::with_gil(|py| {
                Self::run_python_task(
                    py,
                    format,
                    function,
                    input,
                    signal_rx,
                    output_tx.clone(),
                    output_schema_for_python,
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
            state: MapIterStreamState::Running {
                signal: signal_tx,
                python_task,
            },
        }
    }

    fn run_python_task(
        py: Python,
        format: PySparkMapIterFormat,
        function: PyObject,
        input: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        sender: mpsc::Sender<Result<RecordBatch>>,
        output_schema: SchemaRef,
        handle: Handle,
    ) -> PyUdfResult<()> {
        let udf = function.into_bound(py);
        // Create a Python iterator from the input record batch stream and call the UDF.
        // We could have wrap each record batch in a single-element list and call the UDF
        // for each record batch, but that does not work if the user wants to maintain state
        // across record batches.
        let input = PyMapIterInputStream::new(format, input, signal, handle);
        let input = input.into_py(py);
        let output = udf.call1((py.None(), input))?;
        for x in output.iter()? {
            let data = x.and_then(|x| x.get_item(0))?;
            // Ignore empty record batches since the PySpark unit tests expect them to be ignored
            // even if they have incompatible schemas.
            if data.is_empty()? {
                continue;
            }
            let out = format.record_batch_from_py_bound(data, &output_schema);
            if py
                .allow_threads(|| {
                    let out = out.map_err(|e| DataFusionError::External(e.into()));
                    sender.blocking_send(out)
                })
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }
}

impl Drop for MapIterStream {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, MapIterStreamState::Stopped);
        match state {
            MapIterStreamState::Running {
                signal,
                python_task,
            } => {
                let _ = signal.send(());
                // This may block indefinitely waiting for the Python UDF
                // to finish processing all input batches received before the stop signal.
                // Unfortunately, there is no reliable way to abort the thread cleanly,
                // so we have to wait for it to finish.
                let _ = python_task.join();
            }
            MapIterStreamState::Stopped => {}
        }
    }
}

impl Stream for MapIterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for MapIterStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
