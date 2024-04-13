use std::sync::Arc;
use std::any::Any;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, DataType, Field};
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, expr,
    ScalarFunctionDefinition, Volatility,
};

use pyo3::{PyResult, Python, PyObject, ToPyObject};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{IntoPyDict, PyBytes, PyTuple, PyList};
use pyo3::exceptions::PyValueError;

// TODO: Use `use datafusion::arrow::datatypes::{DataType}` instead?
// use framework_proto::spark::connect::DataType as SCDataType;

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
        output_type: ArrowDataType,
        eval_type: i32,
        python_ver: String,
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

    pub fn to_scalar_function(&self) -> expr::Expr {
        expr::Expr::ScalarFunction(expr::ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(self.clone()))),
            args: self.arguments.clone(),
        })
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

    fn return_type(&self, arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        if arg_types != &self.input_types[..] {
            return Err(DataFusionError::Internal(format!("Input types do not match the expected types")));
        }
        Ok(self.output_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // let args = ColumnarValue::values_to_arrays(args)?;

        Python::with_gil(|py| {
            // let binary_sequence = PyBytes::new_bound(py, &self.command);
            // let python_function = PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
            //     .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
            //     .and_then(|f| Ok(f.call1((binary_sequence, ))?.to_object(py)))
            //     // .and_then(|f| Ok(f.call1((binary_sequence, ))))
            //     .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let cloudpickle = PyModule::import_bound(py, "pyspark.cloudpickle") // TODO: make name a variable instead of hardcoding
                .expect("Unable to import 'pyspark.cloudpickle'")
                .getattr("loads")
                .unwrap();

            let binary_sequence = PyBytes::new_bound(py, &self.command);

            let python_function = cloudpickle
                .call1((binary_sequence, ))
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;
            // .to_object(py);
            // .into();

            let py_args = args
                .iter()
                .map(|arg| {
                    match arg {
                        ColumnarValue::Array(arr) => {
                            // Handle this case
                            // unimplemented!()
                            Ok(py.None())
                        }
                        ColumnarValue::Scalar(scalar) => {
                            match scalar {
                                ScalarValue::Null => Ok(py.None()),
                                ScalarValue::Int8(v) => Ok(v.to_object(py)),
                                ScalarValue::Int16(v) => Ok(v.to_object(py)),
                                ScalarValue::Int32(v) => Ok(v.to_object(py)),
                                ScalarValue::Int64(v) => Ok(v.to_object(py)),
                                ScalarValue::UInt8(v) => Ok(v.to_object(py)),
                                ScalarValue::UInt16(v) => Ok(v.to_object(py)),
                                ScalarValue::UInt32(v) => Ok(v.to_object(py)),
                                ScalarValue::UInt64(v) => Ok(v.to_object(py)),
                                ScalarValue::Float32(v) => Ok(v.to_object(py)),
                                ScalarValue::Float64(v) => Ok(v.to_object(py)),
                                ScalarValue::Utf8(v) => Ok(v.to_object(py)),
                                ScalarValue::LargeUtf8(v) => Ok(v.to_object(py)),
                                ScalarValue::Boolean(v) => Ok(v.to_object(py)),
                                ScalarValue::Date32(v) => Ok(v.to_object(py)),
                                ScalarValue::Date64(v) => Ok(v.to_object(py)),
                                ScalarValue::Time32Millisecond(v) => Ok(v.to_object(py)),
                                ScalarValue::Time32Second(v) => Ok(v.to_object(py)),
                                ScalarValue::Time64Microsecond(v) => Ok(v.to_object(py)),
                                ScalarValue::Time64Nanosecond(v) => Ok(v.to_object(py)),
                                ScalarValue::IntervalYearMonth(v) => Ok(v.to_object(py)),
                                ScalarValue::IntervalDayTime(v) => Ok(v.to_object(py)),
                                ScalarValue::DurationSecond(v) => Ok(v.to_object(py)),
                                ScalarValue::DurationMillisecond(v) => Ok(v.to_object(py)),
                                ScalarValue::DurationMicrosecond(v) => Ok(v.to_object(py)),
                                ScalarValue::DurationNanosecond(v) => Ok(v.to_object(py)),
                                ScalarValue::List(v) => {
                                    // Handle this case
                                    // unimplemented!()
                                    Ok(py.None())
                                }
                                _ => Err(PyValueError::new_err(format!("Unsupported scalar value {:?}", scalar))),
                            }
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>();

            let args_tuple = PyTuple::new_bound(py, py_args);
            // Call the Python function with the tuple of arguments
            // let result = python_function
            //     .call1(args_tuple)
            //     .map_err(|e| DataFusionError::Execution(format!("Python Error {:?}", e)))?;

            let py_result = python_function.get_item(0)
                .map_err(|e| DataFusionError::Execution(format!("Python Error {:?}", e)))?
                .call1((1, ))
                .map_err(|e| DataFusionError::Execution(format!("Python Error {:?}", e)))?;
            let rust_result = py_result.extract::<i32>()
                .map_err(|e| DataFusionError::Execution(format!("Python Error {:?}", e)))?;
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(rust_result))))
        })
    }
}