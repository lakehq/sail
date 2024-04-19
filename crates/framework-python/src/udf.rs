use std::any::Any;

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature
    , Volatility,
};
use datafusion_expr::type_coercion::functions::data_types;
use pyo3::prelude::{PyAnyMethods, Python};

use crate::utils::{load_python_function, process_array_ref_with_python_function};

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

        // let args = &args[0];
        // let array_ref = match &args {
        //     ColumnarValue::Array(arr) => {
        //         make_array(arr.into_data())
        //     }
        //     ColumnarValue::Scalar(scalar) => {
        //         unimplemented!("Scalar values are not supported yet")
        //     }
        // };

        let (array_ref, is_scalar) = match &args[0] {
            ColumnarValue::Array(arr) => {
                (arr.clone(), false)
            }
            ColumnarValue::Scalar(scalar) => {
                let arr = scalar.to_array().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to convert scalar to array: {:?}", e))
                })?;
                (arr, true)
            }
        };

        Python::with_gil(|py| {
            let python_function = load_python_function(py, &self.command)?;

            let processed_array = match &self.output_type {
                DataType::Int32 => {
                    process_array_ref_with_python_function::<Int32Type>(&array_ref, py, &python_function)?
                }
                DataType::Int64 => {
                    process_array_ref_with_python_function::<Int64Type>(&array_ref, py, &python_function)?
                }
                _ => return Err(DataFusionError::Internal(format!("Unsupported data type"))),
            };

            if is_scalar {
                // TODO: Implement this
                unimplemented!()
                // Ok(ColumnarValue::Scalar(processed_array))
            } else {
                Ok(ColumnarValue::Array(processed_array))
            }
        })
    }
}
