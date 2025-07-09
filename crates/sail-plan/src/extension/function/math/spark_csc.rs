use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{as_primitive_array, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
#[derive(Debug)]
pub struct SparkCsc {
    signature: Signature,
}

impl Default for SparkCsc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCsc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCsc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_csc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err(
                "spark_csc",
                (1, 1),
                arg_types.len(),
            ));
        }
        let t: &DataType = &arg_types[0];
        match t {
            DataType::Float64
            | DataType::Int64
            | DataType::Int32
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Ok(DataType::Float64),
            DataType::Float32 => Ok(DataType::Float32),

            _ => Err(unsupported_data_type_exec_err(
                "spark_csc",
                "Float32 or Float64",
                t,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if args.len() != 1 {
            return Err(invalid_arg_count_exec_err("spark_csc", (1, 1), args.len()));
        }

        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(x))) => {
                let result: f64 = 1.0 / x.sin();
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
            }

            ColumnarValue::Scalar(ScalarValue::Float32(Some(x))) => {
                let result: f32 = 1.0 / x.sin();
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(result))))
            }
            ColumnarValue::Scalar(ScalarValue::Float32(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(None)))
            }

            ColumnarValue::Array(array) if array.data_type() == &DataType::Float64 => {
                let input = as_primitive_array::<Float64Type>(array);
                let result: Float64Array = input
                    .iter()
                    .map(|opt| opt.map(|v| 1.0 / v.sin()))
                    .collect::<Float64Array>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Array(array) if array.data_type() == &DataType::Float32 => {
                let input = as_primitive_array::<Float32Type>(array);
                let result: Float32Array = input
                    .iter()
                    .map(|opt| opt.map(|v| 1.0 / v.sin()))
                    .collect::<Float32Array>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            other => Err(unsupported_data_type_exec_err(
                "spark_csc",
                "Float32 or Float64",
                &other.data_type(),
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 1 {
            return Err(invalid_arg_count_exec_err("spark_csc", (1, 1), types.len()));
        }

        let t: &DataType = &types[0];
        let valid_type: DataType = match t {
            DataType::Float32 => DataType::Float32,
            DataType::Float64
            | DataType::Int64
            | DataType::Int32
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => DataType::Float64,
            _ => {
                return Err(unsupported_data_type_exec_err(
                    "spark_csc",
                    "Float32 or Float64",
                    t,
                ))
            }
        };

        Ok(vec![valid_type])
    }
}
