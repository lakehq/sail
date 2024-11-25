use std::cmp::Ordering;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::Result;
use futures::{StreamExt, TryStreamExt};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyList;
use pyo3::{intern, PyResult, Python};
use sail_common::udf::MapIterUDF;

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::cereal::PythonFunction;
use crate::config::SparkUdfConfig;
use crate::error::PyUdfResult;
use crate::udf::get_udf_name;
use crate::utils::builtins::PyBuiltins;
use crate::utils::pyarrow::{to_pyarrow_schema, PyArrowRecordBatch, PyArrowToPandasOptions};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkMapIterUDF {
    function_name: String,
    function: Vec<u8>,
    output_schema: SchemaRef,
    use_arrow: bool,
    spark_udf_config: SparkUdfConfig,
    #[allow(dead_code)]
    deterministic: bool,
    #[allow(dead_code)]
    is_barrier: bool,
}

impl PySparkMapIterUDF {
    pub fn new(
        function_name: String,
        function: Vec<u8>,
        output_schema: SchemaRef,
        use_arrow: bool,
        spark_udf_config: SparkUdfConfig,
        deterministic: bool,
        is_barrier: bool,
    ) -> Self {
        let function_name = get_udf_name(&function_name, &function);
        Self {
            function_name,
            function,
            output_schema,
            use_arrow,
            spark_udf_config,
            deterministic,
            is_barrier,
        }
    }
}

#[derive(PartialEq, PartialOrd)]
struct PySparkMapIterUDFOrd<'a> {
    function_name: &'a String,
    function: &'a Vec<u8>,
    use_arrow: bool,
    deterministic: bool,
    is_barrier: bool,
}

impl<'a> From<&'a PySparkMapIterUDF> for PySparkMapIterUDFOrd<'a> {
    fn from(udf: &'a PySparkMapIterUDF) -> Self {
        Self {
            function_name: &udf.function_name,
            function: &udf.function,
            use_arrow: udf.use_arrow,
            deterministic: udf.deterministic,
            is_barrier: udf.is_barrier,
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
        let function = PySparkUdfObject::load(&self.function)?;
        let output_schema = self.output_schema.clone();
        let use_arrow = self.use_arrow;
        let output = input
            .map(move |x| {
                x.and_then(|batch| {
                    Python::with_gil(|py| -> Result<_> {
                        let out = if use_arrow {
                            call_arrow_map_iter_udf(py, &function, batch)?
                        } else {
                            call_pandas_map_iter_udf(py, &function, &output_schema, batch)?
                        };
                        let out: Vec<Result<_>> = out.into_iter().map(Ok).collect();
                        Ok(out)
                    })
                    .map(futures::stream::iter)
                })
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            output,
        )))
    }
}

fn call_arrow_map_iter_udf(
    py: Python,
    function: &PySparkUdfObject,
    batch: RecordBatch,
) -> PyUdfResult<Vec<RecordBatch>> {
    let udf = function.function(py)?;
    let input = batch.to_pyarrow(py)?;
    let input = PyList::new_bound(py, vec![input]);
    let output = udf.call1((py.None(), (input,)))?;
    let output = output
        .iter()?
        .map(|x| {
            let data = x.and_then(|x| x.get_item(0))?;
            if data.len()? == 0 {
                return Ok(None);
            }
            Ok(Some(RecordBatch::from_pyarrow_bound(&data)?))
        })
        .filter_map(|x| x.transpose())
        .collect::<PyUdfResult<Vec<_>>>()?;
    Ok(output)
}

fn call_pandas_map_iter_udf(
    py: Python,
    function: &PySparkUdfObject,
    output_schema: &SchemaRef,
    batch: RecordBatch,
) -> PyUdfResult<Vec<RecordBatch>> {
    let udf = function.function(py)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|x| x.name().clone())
        .collect::<Vec<_>>();
    let pyarrow_schema = to_pyarrow_schema(py, output_schema)?;
    let pyarrow_record_batch_from_pandas =
        PyArrowRecordBatch::from_pandas(py, Some(pyarrow_schema))?;
    let pyarrow_record_batch_to_pandas = PyArrowRecordBatch::to_pandas(
        py,
        PyArrowToPandasOptions {
            // The PySpark unit tests do not expect Pandas nullable types.
            use_pandas_nullable_types: false,
        },
    )?;
    let py_str = PyBuiltins::str(py)?;
    let py_isinstance = PyBuiltins::isinstance(py)?;
    let input = batch.to_pyarrow(py)?;
    let input = pyarrow_record_batch_to_pandas.call1((input,))?;
    let input = PyList::new_bound(py, vec![input]);
    let output = udf.call1((py.None(), (input,)))?;
    let output = output
        .iter()?
        .map(|x| {
            let data = x.and_then(|x| x.get_item(0))?;
            if data.len()? == 0 {
                return Ok(None);
            }
            let data = if data
                .getattr(intern!(py, "columns"))?
                .iter()?
                .map(|c| py_isinstance.call1((c?, &py_str))?.extract())
                .collect::<PyResult<Vec<bool>>>()?
                .iter()
                .any(|x| *x)
            {
                data
            } else {
                let selected = data
                    .getattr(intern!(py, "columns"))?
                    .iter()?
                    .take(columns.len())
                    .collect::<PyResult<Vec<_>>>()?;
                let data = data.get_item(selected)?;
                data.setattr(intern!(py, "columns"), columns.clone())?;
                data
            };
            let data = pyarrow_record_batch_from_pandas.call1((data,))?;
            Ok(Some(RecordBatch::from_pyarrow_bound(&data)?))
        })
        .filter_map(|x| x.transpose())
        .collect::<PyUdfResult<Vec<_>>>()?;
    Ok(output)
}
