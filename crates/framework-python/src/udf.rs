use std::sync::Arc;
use std::any::Any;
use datafusion::arrow;

use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef, PrimitiveArray};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use datafusion::common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, expr,
    ScalarFunctionDefinition, Volatility, TypeSignature,
};
use datafusion_expr::type_coercion::functions::data_types;

use pyo3::{PyResult, Python, PyObject, ToPyObject, PyAny};
use pyo3::prelude::{PyModule, Py};
use pyo3::types::{IntoPyDict, PyBytes, PyTuple, PyList};
use pyo3::exceptions::PyValueError;

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,

    // TODO: See what I exactly need. This is a placeholder.
    function_name: String,
    output_type: DataType,
    eval_type: i32,
    command: Vec<u8>,
    python_ver: String,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        input_types: Vec<DataType>,
        command: Vec<u8>,
        output_type: DataType,
        eval_type: i32, // TODO: Incorporate this
        python_ver: String, // TODO: Incorporate this
    ) -> Self {
        Self {
            signature: Signature::exact(
                input_types.clone(),
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            command,
            output_type,
            eval_type,
            python_ver,
        }
    }
}

impl ScalarUDFImpl for PythonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.function_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        data_types(arg_types, &self.signature())
            .map_err(|e|
                DataFusionError::Internal(format!("Input types do not match the expected types {:?}", e))
            )?;

        Ok(self.output_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let input_types = match &self.signature().type_signature {
            TypeSignature::Exact(types) => types,
            _ => {
                return Err(DataFusionError::Internal(format!("Type Signature is not an exact")));
            }
        };
        println!("TypeSignature input_types: {:?}", input_types);
        println!("self.output_type: {:?}", self.output_type);
        println!("self.eval_type: {:?}", self.eval_type);
        println!("args: {:?}", args);

        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
                self.name()
            )));
        }
        let args = &args[0];

        let args_vec = match &args {
            ColumnarValue::Array(arr) => {
                let arr_data_type = arr.as_ref().data_type();
                match arr_data_type {
                    DataType::Int64 => {
                        let vec = arr
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap()
                            .values()
                            .to_vec();
                        vec
                    }
                    DataType::Int32 => {
                        unimplemented!()
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unsupported data type {:?}",
                            args.data_type()
                        )));
                    }
                }
            }
            ColumnarValue::Scalar(scalar) => {
                unimplemented!("Scalar values are not supported yet")
            }
        };
        println!("args_vec: {:?}", args_vec);

        Python::with_gil(|py| {
            let binary_sequence = PyBytes::new(py, &self.command);
            println!("binary_sequence: {:?}", binary_sequence);

            let python_function_tuple = PyModule::import(py, pyo3::intern!(py, "pyspark.cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
                .and_then(|loads| Ok(loads.call1((binary_sequence, ))?))
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let python_function = python_function_tuple.get_item(0)
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let python_function_return_type = python_function_tuple.get_item(1)
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            println!("python_function: {:?}", python_function);
            println!("python_function_return_type: {:?}", python_function_return_type);

            if !python_function.is_callable() {
                return Err(DataFusionError::Execution("Expected a callable Python function".to_string()));
            }

            let mut results = Vec::new();
            for arg in &args_vec {
                let args_tuple = PyTuple::new(py, &[arg]);
                let py_result = python_function
                    .call1(args_tuple)
                    .map_err(|e| DataFusionError::Execution(format!("py_result Python Error {:?}", e)))?;
                let rust_result = match self.output_type {
                    DataType::Int32 => {
                        let value = py_result.extract::<i32>()
                            .map_err(|e| DataFusionError::Execution(format!("rust_result Python Error {:?}", e)))?;
                        // Ok(ScalarValue::Int32(Some(value)))
                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(value))))
                    }
                    DataType::Int64 => {
                        let value = py_result.extract::<i64>()
                            .map_err(|e| DataFusionError::Execution(format!("rust_result Python Error {:?}", e)))?;
                        // Ok(ScalarValue::Int64(Some(value)))
                        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value))))
                    }
                    _ => Err(DataFusionError::Internal("Unsupported output type".to_string())),
                }?;
                results.push(rust_result);
            }

            let array_ref = match results[0] {
                ColumnarValue::Scalar(ScalarValue::Int64(_)) => {
                    let array_data = arrow::array::Int64Array::from_iter_values(
                        results.into_iter().filter_map(|value| match value {
                            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some(v),
                            _ => None,
                        })
                    );
                    Arc::new(array_data) as ArrayRef
                }
                ColumnarValue::Scalar(ScalarValue::Int32(_)) => {
                    let array_data = arrow::array::Int32Array::from_iter_values(
                        results.into_iter().filter_map(|value| match value {
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Some(v),
                            _ => None,
                        })
                    );
                    Arc::new(array_data) as ArrayRef
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type"
                    )));
                }
            };
            Ok(ColumnarValue::Array(array_ref))
        })
    }
}