use std::sync::Arc;
use std::any::Any;
use datafusion::arrow;

use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Int32Type};
use datafusion::arrow::array::{make_array, Array, ArrayData, ArrayRef, PrimitiveArray, ArrowPrimitiveType, Int32Array, Int64Array, Float32Array, Float64Array};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::common::cast::{as_large_list_array, as_list_array, as_map_array};
use datafusion_expr::{
    ColumnarValue, FuncMonotonicity, ScalarUDF, ScalarUDFImpl, Signature, expr,
    ScalarFunctionDefinition, Volatility, TypeSignature,
};
use datafusion_expr::type_coercion::functions::data_types;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::{IntoPy, PyClass};
use crate::utils::{process_array_ref_with_python_function, get_native_values_from_array};

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
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
                self.name()
            )));
        }

        let args = &args[0];

        let array_ref = match &args {
            ColumnarValue::Array(arr) => {
                make_array(arr.into_data())
            }
            ColumnarValue::Scalar(scalar) => {
                unimplemented!("Scalar values are not supported yet")
            }
        };

        Python::with_gil(|py| {
            let binary_sequence = PyBytes::new_bound(py, &self.command);

            let python_function_tuple = PyModule::import_bound(py, pyo3::intern!(py, "pyspark.cloudpickle"))
                .and_then(|cloudpickle| cloudpickle.getattr(pyo3::intern!(py, "loads")))
                .and_then(|loads| Ok(loads.call1((binary_sequence, ))?))
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let python_function = python_function_tuple.get_item(0)
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            let python_function_return_type = python_function_tuple.get_item(1)
                .map_err(|e| DataFusionError::Execution(format!("Pickle Error {:?}", e)))?;

            if !python_function.is_callable() {
                return Err(DataFusionError::Execution("Expected a callable Python function".to_string()));
            }

            let result_array = match &self.output_type {
                DataType::Int32 => {
                    process_array_ref_with_python_function::<Int32Type>(&array_ref, py, &python_function)?
                }
                DataType::Int64 => {
                    process_array_ref_with_python_function::<Int64Type>(&array_ref, py, &python_function)?
                }
                _ => return Err(DataFusionError::Internal(format!("Unsupported data type"))),
            };
            Ok(ColumnarValue::Array(result_array))
        })
    }
}
