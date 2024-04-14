use std::sync::Arc;
use std::any::Any;
use datafusion::arrow;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, DataType, Field, Int64Type};
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef, PrimitiveArray};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::arrow::pyarrow::{FromPyArrow, PyArrowType, ToPyArrow};
use datafusion::common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, expr,
    ScalarFunctionDefinition, Volatility,
};

use pyo3::{PyResult, Python, PyObject, ToPyObject, PyAny};
use pyo3::prelude::{PyModule, Py};
use pyo3::types::{IntoPyDict, PyBytes, PyTuple, PyList};
use pyo3::exceptions::PyValueError;

#[derive(Debug, Clone)]
pub struct PythonUDF {
    signature: Signature,

    // TODO: See what I exactly need. This is a placeholder.
    function_name: String,
    arguments: Vec<expr::Expr>,
    input_types: Vec<ArrowDataType>,
    output_type: ArrowDataType,
    eval_type: i32,
    command: Vec<u8>,
    python_ver: String,
}

impl PythonUDF {
    pub fn new(
        function_name: String,
        deterministic: bool,
        arguments: Vec<expr::Expr>,
        input_types: Vec<ArrowDataType>,
        command: Vec<u8>,
        // command_fnc: u8,
        // command_fnc_return_type: u8,
        output_type: ArrowDataType,
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
            arguments,
            input_types,
            command,
            output_type,
            eval_type,
            python_ver,
        }
    }

    // pub fn to_scalar_function(&self) -> expr::Expr {
    //     expr::Expr::ScalarFunction(expr::ScalarFunction {
    //         func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(self.clone()))),
    //         args: self.arguments.clone(),
    //     })
    // }
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

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != &self.input_types[..] {
            return Err(DataFusionError::Internal(format!("Input types do not match the expected types")));
        }
        // TODO: Use `self.output_type` and `python_function_return_type` (needs to be unpickled))
        Ok(self.output_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // let args = ColumnarValue::values_to_arrays(args)?;
        println!("args: {:?}", args);
        println!("self.input_types: {:?}", self.input_types);
        println!("self.output_type: {:?}", self.output_type);
        println!("self.eval_type: {:?}", self.eval_type);

        let args = ColumnarValue::values_to_arrays(args)?;
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
                self.name()
            )));
        }
        let args = &args[0];
        println!("args after values_to_arrays: {:?}", args);
        println!("&args[0]: {:?}", args);

        // let args_vec: Vec<_>;
        let args_vec = match args.data_type() {
            ArrowDataType::Int64 => {
                let arrow_arr = args
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                println!("array: {:?}", arrow_arr);
                let array_vec = arrow_arr
                    .values()
                    .to_vec();
                println!("array_vec: {:?}", array_vec);
                array_vec
            }
            ArrowDataType::Int32 => {
                unimplemented!()
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?}",
                    args.data_type()
                )));
            }
        };
        println!("args_vec: {:?}", args_vec);
        // let args_vec = args_vec
        //     .iter()
        //     .map(|&value| vec![value])
        //     .collect::<Vec<_>>();
        // let args_vec = args_vec.iter().map(|&value| vec![value]).collect();
        println!("args_vec: {:?}", args_vec);

        Python::with_gil(|py| {
            // let binary_sequence = PyBytes::new(py, &self.command);
            // let python_function = PyModule::import(py, pyo3::intern!(py, "pyspark.cloudpickle"))
            //     .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
            //     .and_then(|f| Ok(f.call1((binary_sequence, ))?.to_object(py)))
            //     // .and_then(|f| Ok(f.call1((binary_sequence, ))))
            //     .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let cloudpickle = PyModule::import(py, "pyspark.cloudpickle") // TODO: make name a variable instead of hardcoding
                .expect("Unable to import 'pyspark.cloudpickle'")
                .getattr("loads")
                .unwrap();

            let binary_sequence = PyBytes::new(py, &self.command);
            println!("binary_sequence: {:?}", binary_sequence);

            // let python_function: Py<PyAny> = cloudpickle
            let python_function_tuple = cloudpickle
                .call1((binary_sequence, ))
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;
            // .to_object(py);
            // .unwrap()

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
                    ArrowDataType::Int32 => {
                        let value = py_result.extract::<i32>()
                            .map_err(|e| DataFusionError::Execution(format!("rust_result Python Error {:?}", e)))?;
                        // Ok(ScalarValue::Int32(Some(value)))
                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(value))))
                    }
                    ArrowDataType::Int64 => {
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