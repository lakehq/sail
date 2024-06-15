use std::any::Any;

use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use pyo3::{
    prelude::*,
    types::{PyDict, PyIterator, PyList, PyTuple},
};

use crate::cereal::partial_pyspark_udf::{
    is_pyspark_arrow_udf, is_pyspark_pandas_udf, PartialPySparkUDF,
};
use crate::pyarrow::{FromPyArrow, ToPyArrow};
use crate::udf::CommonPythonUDF;

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
            output_type,
            eval_type,
            python_function,
        }
    }
}

impl CommonPythonUDF for PySparkUDF {
    type PythonFunctionType = PartialPySparkUDF;

    fn python_function(&self) -> &Self::PythonFunctionType {
        &self.python_function
    }

    fn output_type(&self) -> &DataType {
        &self.output_type
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
        let args: Vec<ArrayRef> = ColumnarValue::values_to_arrays(args)?;

        if is_pyspark_arrow_udf(&self.eval_type) {
            let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
                let pyarrow_module_array: Bound<PyAny> =
                    self.get_pyarrow_module_array_function(py)?;
                let builtins_list: Bound<PyAny> = self.get_python_builtins_list_function(py)?;
                let python_function: Bound<PyAny> = self.get_python_function(py)?;
                let pyarrow_output_data_type: Bound<PyAny> =
                    self.get_pyarrow_output_data_type(py)?;
                let output_data_type_kwargs: Bound<PyDict> =
                    self.build_pyarrow_module_array_kwargs(py, pyarrow_output_data_type, false)?;

                let py_args: Vec<Bound<PyAny>> = args
                    .iter()
                    .map(|arg| {
                        arg.into_data()
                            .to_pyarrow(py)
                            .unwrap()
                            .clone_ref(py)
                            .into_bound(py)
                    })
                    .collect::<Vec<_>>();
                let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);

                let results: Bound<PyAny> = python_function
                    .call1((py.None(), (py_args,)))
                    .map_err(|e| {
                        DataFusionError::Execution(format!("PySpark UDF Result: {e:?}"))
                    })?;
                let results: Bound<PyAny> = builtins_list
                    .call1((results,))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("Error calling list(): {:?}", err))
                    })?
                    .get_item(0)
                    .map_err(|err| {
                        DataFusionError::Internal(format!(
                            "Result list() first get_item(0): {:?}",
                            err
                        ))
                    })?;

                let results_data: Bound<PyAny> = results.get_item(0).map_err(|err| {
                    DataFusionError::Internal(format!("Result list get_item(0): {:?}", err))
                })?;
                let _results_datatype: Bound<PyAny> = results.get_item(1).map_err(|err| {
                    DataFusionError::Internal(format!("Result list get_item(0): {:?}", err))
                })?;
                let results_data: Bound<PyAny> = pyarrow_module_array
                    .call((results_data,), Some(&output_data_type_kwargs))
                    .map_err(|err| DataFusionError::Internal(format!("Result array {:?}", err)))?;

                let array_data = ArrayData::from_pyarrow_bound(&results_data)
                    .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))?;
                Ok(array_data)
            });
            return Ok(ColumnarValue::Array(make_array(array_data?)));
        }

        if is_pyspark_pandas_udf(&self.eval_type) {
            let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
                let pyarrow_module_array: Bound<PyAny> =
                    self.get_pyarrow_module_array_function(py)?;
                let builtins_list: Bound<PyAny> = self.get_python_builtins_list_function(py)?;
                let python_function: Bound<PyAny> = self.get_python_function(py)?;
                let pyarrow_output_data_type: Bound<PyAny> =
                    self.get_pyarrow_output_data_type(py)?;
                let output_data_type_kwargs: Bound<PyDict> =
                    self.build_pyarrow_module_array_kwargs(py, pyarrow_output_data_type, true)?;

                let py_args: Vec<Bound<PyAny>> = args
                    .iter()
                    .map(|arg| {
                        arg.into_data()
                            .to_pyarrow(py)
                            .unwrap()
                            .call_method0(py, pyo3::intern!(py, "to_pandas"))
                            .unwrap()
                            .clone_ref(py)
                            .into_bound(py)
                    })
                    .collect::<Vec<_>>();
                let py_args: Bound<PyTuple> = PyTuple::new_bound(py, &py_args);

                let results: Bound<PyAny> = python_function
                    .call1((py.None(), (py_args,)))
                    .map_err(|e| {
                        DataFusionError::Execution(format!("PySpark UDF Result: {e:?}"))
                    })?;
                let results: Bound<PyAny> = builtins_list
                    .call1((results,))
                    .map_err(|err| {
                        DataFusionError::Internal(format!("Error calling list(): {:?}", err))
                    })?
                    .get_item(0)
                    .map_err(|err| {
                        DataFusionError::Internal(format!(
                            "Result list() first get_item(0): {:?}",
                            err
                        ))
                    })?;

                let results_data: Bound<PyAny> = results.get_item(0).map_err(|err| {
                    DataFusionError::Internal(format!("Result list get_item(0): {:?}", err))
                })?;
                let _results_datatype: Bound<PyAny> = results.get_item(1).map_err(|err| {
                    DataFusionError::Internal(format!("Result list get_item(0): {:?}", err))
                })?;
                let results_data: Bound<PyAny> = pyarrow_module_array
                    .call((results_data,), Some(&output_data_type_kwargs))
                    .map_err(|err| DataFusionError::Internal(format!("Result array {:?}", err)))?;

                let array_data = ArrayData::from_pyarrow_bound(&results_data)
                    .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))?;
                Ok(array_data)
            });
            return Ok(ColumnarValue::Array(make_array(array_data?)));
        }

        let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
            let pyarrow_module_array: Bound<PyAny> = self.get_pyarrow_module_array_function(py)?;
            let builtins_list: Bound<PyAny> = self.get_python_builtins_list_function(py)?;
            let python_function: Bound<PyAny> = self.get_python_function(py)?;
            let pyarrow_output_data_type: Bound<PyAny> = self.get_pyarrow_output_data_type(py)?;
            let output_data_type_kwargs: Bound<PyDict> =
                self.build_pyarrow_module_array_kwargs(py, pyarrow_output_data_type, false)?;

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

            let results: Vec<Bound<PyAny>> = py_args
                .map(|py_arg| -> Result<Bound<PyAny>, DataFusionError> {
                    let result: Bound<PyAny> = python_function
                        .call1((py.None(), (py_arg.unwrap(),)))
                        .map_err(|e| {
                            DataFusionError::Execution(format!("PySpark UDF Result: {e:?}"))
                        })?;
                    let result: Bound<PyAny> = builtins_list
                        .call1((result,))
                        .map_err(|err| {
                            DataFusionError::Internal(format!("Error calling list(): {:?}", err))
                        })?
                        .get_item(0)
                        .map_err(|err| {
                            DataFusionError::Internal(format!(
                                "Result list() first get_item(0): {:?}",
                                err
                            ))
                        })?;
                    Ok(result)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    DataFusionError::Internal(format!("PySpark UDF Results: {:?}", err))
                })?;

            let results: Bound<PyAny> = pyarrow_module_array
                .call((results,), Some(&output_data_type_kwargs))
                .map_err(|err| DataFusionError::Internal(format!("Result array {:?}", err)))?;

            ArrayData::from_pyarrow_bound(&results)
                .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))
        });

        Ok(ColumnarValue::Array(make_array(array_data?)))
    }

    fn invoke_no_args(&self, _number_rows: usize) -> Result<ColumnarValue> {
        let array_data: Result<ArrayData, DataFusionError> = Python::with_gil(|py| {
            let pyarrow_module_array: Bound<PyAny> = self.get_pyarrow_module_array_function(py)?;
            let builtins_list: Bound<PyAny> = self.get_python_builtins_list_function(py)?;
            let python_function: Bound<PyAny> = self.get_python_function(py)?;
            let pyarrow_output_data_type: Bound<PyAny> = self.get_pyarrow_output_data_type(py)?;
            let output_data_type_kwargs: Bound<PyDict> =
                self.build_pyarrow_module_array_kwargs(py, pyarrow_output_data_type, false)?;

            let results: Bound<PyAny> = python_function
                .call1((py.None(), (PyList::empty_bound(py),)))
                .map_err(|e| DataFusionError::Execution(format!("PySpark UDF Result: {e:?}")))?;

            let results: Bound<PyAny> = builtins_list
                .call1((results,))
                .map_err(|err| {
                    DataFusionError::Internal(format!("Error calling list(): {:?}", err))
                })?
                .get_item(0)
                .map_err(|err| {
                    DataFusionError::Internal(format!("Result list() first get_item(0): {:?}", err))
                })?;

            let results: Bound<PyAny> = pyarrow_module_array
                .call(([results],), Some(&output_data_type_kwargs))
                .map_err(|err| DataFusionError::Internal(format!("Result array {:?}", err)))?;

            ArrayData::from_pyarrow_bound(&results)
                .map_err(|err| DataFusionError::Internal(format!("array_data {:?}", err)))
        });

        Ok(ColumnarValue::Array(make_array(array_data?)))
    }
}
