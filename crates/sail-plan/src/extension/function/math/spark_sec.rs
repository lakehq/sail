use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug)]
pub struct SparkSec {
    signature: Signature,
}

impl Default for SparkSec {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSec {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_sec"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Float32) => Ok(DataType::Float32),
            Some(DataType::Float64) => Ok(DataType::Float64),
            _ => Ok(DataType::Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [arg] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("spark_sec", (1, 1), args.len()));
        };

        match arg {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(value.map(sec_f64)),
            )),
            ColumnarValue::Scalar(ScalarValue::Float32(value)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float32(value.map(sec_f32)),
            )),
            ColumnarValue::Array(array) if array.data_type() == &DataType::Float64 => {
                let array = array.as_primitive::<Float64Type>();
                let result = array
                    .iter()
                    .map(|opt| opt.map(sec_f64))
                    .collect::<datafusion::arrow::array::Float64Array>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Array(array) if array.data_type() == &DataType::Float32 => {
                let array = array.as_primitive::<datafusion::arrow::datatypes::Float32Type>();
                let result = array
                    .iter()
                    .map(|opt| opt.map(sec_f32))
                    .collect::<datafusion::arrow::array::Float32Array>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            other => Err(unsupported_data_type_exec_err(
                "spark_sec",
                "Float32 or Float64",
                &other.data_type(),
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 1 {
            return Err(invalid_arg_count_exec_err("spark_sec", (1, 1), types.len()));
        }

        let t: &DataType = &types[0];

        let coerced = match t {
            DataType::Float64 => DataType::Float64,
            DataType::Float32 => DataType::Float32,
            DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => DataType::Float64,
            _ => {
                return Err(unsupported_data_types_exec_err(
                    "spark_sec",
                    "Float32 | Float64 | Integer | Decimal",
                    types,
                ));
            }
        };

        Ok(vec![coerced])
    }
}

fn sec_f64(value: f64) -> f64 {
    1.0 / value.cos()
}

fn sec_f32(value: f32) -> f32 {
    1.0 / value.cos()
}
