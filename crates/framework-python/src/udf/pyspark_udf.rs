use std::any::Any;

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::types::{PyList, PyTuple};
use pyo3::{
    prelude::*,
    types::{PyDict, PyIterator},
};

use crate::cereal::partial_pyspark_udf::{
    is_pyspark_arrow_udf, is_pyspark_pandas_udf, PartialPySparkUDF,
};
use crate::pyarrow::{FromPyArrow, ToPyArrow};

#[derive(Debug, Clone)]
pub struct PySparkUDF {
    signature: Signature,
    function_name: String,
    output_type: DataType,
    eval_type: i32,
    python_function: PartialPySparkUDF,
}

impl PySparkUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        eval_type: i32,
        python_function: PartialPySparkUDF,
        output_type: DataType,
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types,
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            eval_type,
            python_function,
            output_type,
        }
    }
}

impl ScalarUDFImpl for PySparkUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.function_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        // if is_pyspark_arrow_udf(self.eval_type) || is_pyspark_pandas_udf(self.eval_type) {
        //     // TODO: Separate this into a new UDF.
        //     let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
        //         let pyarrow_module = PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
        //             .map_err(|err| {
        //                 DataFusionError::Internal(format!("pyarrow import error: {:?}", err))
        //             })?;
        //
        //         let python_function: Bound<PyAny> = self
        //             .python_function
        //             .0
        //             .clone_ref(py)
        //             .into_bound(py)
        //             .get_item(0)
        //             .map_err(|err| {
        //                 DataFusionError::Internal(format!("python_function {:?}", err))
        //             })?;
        //
        //         ArrayData::from_pyarrow_bound(&result)
        //             .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))
        //     });
        // }

        let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
            let pyarrow_module =
                PyModule::import_bound(py, pyo3::intern!(py, "pyarrow")).map_err(|err| {
                    DataFusionError::Internal(format!("pyarrow import error: {:?}", err))
                })?;

            let python_function: Bound<PyAny> = self
                .python_function
                .0
                .clone_ref(py)
                .into_bound(py)
                .get_item(0)
                .map_err(|err| DataFusionError::Internal(format!("python_function {:?}", err)))?;

            let py_args_columns_list: Vec<Bound<PyAny>> = args
                .iter()
                .map(|arg| {
                    arg.into_data()
                        .to_pyarrow(py)
                        .unwrap()
                        .call_method0(py, pyo3::intern!(py, "to_pylist"))
                        .unwrap()
                        .clone_ref(py)
                        .into_bound(py)
                })
                .collect::<Vec<_>>();
            let py_args_tuple: Bound<PyTuple> = PyTuple::new_bound(py, &py_args_columns_list);
            // TODO: Do zip in Rust for performance.
            let py_args_zip: Bound<PyAny> = py
                .eval_bound("zip", None, None)
                .map_err(|err| {
                    DataFusionError::Internal(format!("py_args_zip eval_bound{:?}", err))
                })?
                .call1(&py_args_tuple)
                .map_err(|err| DataFusionError::Internal(format!("py_args_zip zip{:?}", err)))?;
            let py_args: Bound<PyIterator> = PyIterator::from_bound_object(&py_args_zip)
                .map_err(|err| DataFusionError::Internal(format!("py_args_iter {:?}", err)))?;

            let pyarrow_output_type: PyObject = self.output_type.to_pyarrow(py).map_err(|err| {
                DataFusionError::Internal(format!("output_type to_pyarrow {:?}", err))
            })?;

            let output_type_kwargs: Bound<PyDict> = PyDict::new_bound(py);
            output_type_kwargs
                .set_item("type", pyarrow_output_type.clone_ref(py))
                .map_err(|err| DataFusionError::Internal(format!("kwargs {:?}", err)))?;

            let results: Vec<Bound<PyAny>> = py_args
                .map(|py_arg| -> Result<Bound<PyAny>, DataFusionError> {
                    let result: Bound<PyAny> = python_function
                        .call1((py.None(), (py_arg.unwrap(),)))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("PySpark UDF Result: {e:?}"))
                        })?;
                    println!("CHECK HERE Result: {:?}", result);
                    let result: Bound<PyAny> = py
                        .eval_bound("list", None, None)
                        .map_err(|err| {
                            DataFusionError::Internal(format!("eval_bound list: {:?}", err))
                        })?
                        .call1((result,))
                        .map_err(|err| {
                            DataFusionError::Internal(format!("Call eval_bound list: {:?}", err))
                        })?
                        // // TODO: Ensure result list has only one item.
                        .get_item(0)
                        .map_err(|err| {
                            DataFusionError::Internal(format!("Result list get_item: {:?}", err))
                        })?;
                    println!("CHECK HERE result after list: {:?}", result);
                    Ok(result)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDF Results: {:?}", err))
                })?;

            let result: Bound<PyAny> = pyarrow_module
                .getattr(pyo3::intern!(py, "array"))
                .and_then(|array| array.call((results,), Some(&output_type_kwargs)))
                .map_err(|err| {
                    DataFusionError::Internal(format!("Result concat_arrays {:?}", err))
                })?;

            ArrayData::from_pyarrow_bound(&result)
                .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))
        });

        Ok(ColumnarValue::Array(make_array(array_data?)))
    }
}
