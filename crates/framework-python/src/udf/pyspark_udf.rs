use std::any::Any;

use crate::cereal::partial_pyspark_udf::PartialPySparkUDF;
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::types::{PyList, PyTuple};
use pyo3::{
    prelude::*,
    types::{PyDict, PyIterator},
};

use crate::pyarrow::{FromPyArrow, ToPyArrow};

#[derive(Debug, Clone)]
pub struct PySparkUDF {
    signature: Signature,
    function_name: String,
    output_type: DataType,
    python_function: PartialPySparkUDF,
}

impl PySparkUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
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
        let processed_array: Result<ArrayRef, DataFusionError> = Python::with_gil(|py| {
            let pyarrow_module =
                PyModule::import_bound(py, pyo3::intern!(py, "pyarrow")).map_err(|err| {
                    DataFusionError::Internal(format!("pyarrow import error: {:?}", err))
                })?;

            let python_function = self
                .python_function
                .0
                .clone_ref(py)
                .into_bound(py)
                .get_item(0)
                .map_err(|err| DataFusionError::Internal(format!("python_function {:?}", err)))?;

            let py_args_columns_list = args
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
            let py_args_tuple = PyTuple::new_bound(py, &py_args_columns_list);
            // TODO: Do zip in Rust for performance.
            let py_args_zip = py
                .eval_bound("zip", None, None)
                .map_err(|err| {
                    DataFusionError::Internal(format!("py_args_zip eval_bound{:?}", err))
                })?
                .call1(&py_args_tuple)
                .map_err(|err| DataFusionError::Internal(format!("py_args_zip zip{:?}", err)))?;
            let py_args = PyIterator::from_bound_object(&py_args_zip)
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
                    let result = python_function
                        .call1((py.None(), (py_arg.unwrap(),)))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("PySpark UDF Result: {e:?}"))
                        })?;
                    println!("CHECK HERE Result: {:?}", result);
                    let result = py
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
                    // let result = pyarrow_module
                    //     .getattr(pyo3::intern!(py, "array"))
                    //     // .and_then(|array| array.call((result,), Some(&output_type_kwargs)))
                    //     .and_then(|array| array.call1((result,)))
                    //     .map_err(|err| {
                    //         DataFusionError::Internal(format!("Adjust result: {:?}", err))
                    //     })?;
                    // println!("CHECK HERE adjusted_result: {:?}", result);
                    Ok(result)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDF Results: {:?}", err))
                })?;

            // let result: Bound<PyAny> = pyarrow_module
            //     .getattr(pyo3::intern!(py, "concat_arrays"))
            //     .and_then(|concat_arrays| concat_arrays.call1((results,)))
            //     .map_err(|err| {
            //         DataFusionError::Internal(format!("Result concat_arrays {:?}", err))
            //     })?;
            let result: Bound<PyAny> = pyarrow_module
                .getattr(pyo3::intern!(py, "array"))
                .and_then(|array| array.call((results,), Some(&output_type_kwargs)))
                .map_err(|err| {
                    DataFusionError::Internal(format!("Result concat_arrays {:?}", err))
                })?;

            let array_data = ArrayData::from_pyarrow_bound(&result)
                .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))?;

            let array = make_array(array_data);
            Ok(array)
        });

        Ok(ColumnarValue::Array(processed_array?))
    }
}
