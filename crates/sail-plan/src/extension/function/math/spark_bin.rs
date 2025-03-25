use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};

#[derive(Debug)]
pub struct SparkBin {
    signature: Signature,
}

impl Default for SparkBin {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBin {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(invalid_arg_count_exec_err("bin", (1, 1), args.args.len()));
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value.map(bin))))
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Int64 => {
                    let array = array.as_primitive::<Int64Type>();
                    let result = if array.null_count() == 0 {
                        // Fast path: No nulls
                        StringArray::from_iter_values(
                            array.values().iter().map(|value| bin(*value)),
                        )
                    } else {
                        // Safe path: Has nulls
                        array.iter().map(|v| v.map(bin)).collect()
                    };
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                other => Err(unsupported_data_type_exec_err(
                    "bin",
                    format!("{}", DataType::Int64).as_str(),
                    other,
                )),
            },
            other => Err(unsupported_data_type_exec_err(
                "bin",
                format!("{}", DataType::Int64).as_str(),
                &other.data_type(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("bin", (1, 1), arg_types.len()));
        }
        Ok(vec![DataType::Int64])
    }
}

fn bin(value: i64) -> String {
    // TODO: This is not performant, but it's a simple implementation
    if value >= 0 {
        format!("{:b}", value)
    } else {
        format!("{:064b}", value)
    }
}
