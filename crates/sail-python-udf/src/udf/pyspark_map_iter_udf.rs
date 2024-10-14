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
use pyo3::{intern, Python};
use sail_common::udf::MapIterUDF;

use crate::cereal::pyspark_udf::PySparkUdfObject;
use crate::cereal::PythonFunction;
use crate::config::SparkUdfConfig;
use crate::error::PyUdfResult;
use crate::udf::{
    build_pyarrow_record_batch_kwargs, build_pyarrow_to_pandas_kwargs,
    get_pyarrow_record_batch_from_pandas_function, get_pyarrow_schema, get_udf_name,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PySparkMapIterUDF {
    function_name: String,
    function: Vec<u8>,
    output_schema: SchemaRef,
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
        spark_udf_config: SparkUdfConfig,
        deterministic: bool,
        is_barrier: bool,
    ) -> Self {
        let function_name = get_udf_name(&function_name, &function);
        Self {
            function_name,
            function,
            output_schema,
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
    deterministic: bool,
    is_barrier: bool,
}

impl<'a> From<&'a PySparkMapIterUDF> for PySparkMapIterUDFOrd<'a> {
    fn from(udf: &'a PySparkMapIterUDF) -> Self {
        Self {
            function_name: &udf.function_name,
            function: &udf.function,
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
        let output = input
            .map(move |x| {
                x.and_then(|batch| {
                    Python::with_gil(|py| -> Result<_> {
                        let out: Vec<Result<_>> =
                            call_map_iter_udf(py, &function, &output_schema, batch)?
                                .into_iter()
                                .map(Ok)
                                .collect();
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

fn call_map_iter_udf(
    py: Python,
    function: &PySparkUdfObject,
    output_schema: &SchemaRef,
    batch: RecordBatch,
) -> PyUdfResult<Vec<RecordBatch>> {
    let pyarrow_record_batch_from_pandas = get_pyarrow_record_batch_from_pandas_function(py)?;
    let pyarrow_to_pandas_kwargs = build_pyarrow_to_pandas_kwargs(py)?;
    let pyarrow_schema = get_pyarrow_schema(output_schema, py)?;
    let pyarrow_record_batch_kwargs = build_pyarrow_record_batch_kwargs(py, pyarrow_schema)?;
    let input = batch.to_pyarrow(py)?.call_method_bound(
        py,
        intern!(py, "to_pandas"),
        (),
        Some(&pyarrow_to_pandas_kwargs),
    )?;
    let input = PyList::new_bound(py, vec![input]);
    let output = function.function(py)?.call1((py.None(), (input,)))?;
    let output = output
        .iter()?
        .map(|x| {
            let data = x?;
            let data = data.get_item(0)?;
            let data = pyarrow_record_batch_from_pandas
                .call((data,), Some(&pyarrow_record_batch_kwargs))?;
            Ok(RecordBatch::from_pyarrow_bound(&data)?)
        })
        .collect::<PyUdfResult<Vec<_>>>()?;
    Ok(output)
}
