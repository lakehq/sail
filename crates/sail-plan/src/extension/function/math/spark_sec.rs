use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::unsupported_data_type_exec_err;

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
            signature: Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
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
        match arg_types[0] {
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Float32 => Ok(DataType::Float32),
            _ => internal_err!("Unsupported data type {} for sec", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [arg] = args.as_slice() else {
            return exec_err!(
                "Spark `sec` function requires 1 argument, got {}",
                args.len()
            );
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
                "sec",
                "Float32 or Float64",
                &other.data_type(),
            )),
        }
    }
}

fn sec_f64(value: f64) -> f64 {
    1.0 / value.cos()
}

fn sec_f32(value: f32) -> f32 {
    1.0 / value.cos()
}
