use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkBRound {
    signature: Signature,
}

impl Default for SparkBRound {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBRound {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBRound {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_bround"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return exec_err!("spark_bround expects 2 arguments, got {}", arg_types.len());
        }

        match &arg_types[0] {
            DataType::Float64
            | DataType::Float32
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Ok(DataType::Float64),
            other => exec_err!("Unsupported type for spark_bround: {}", other),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return exec_err!(
                "spark_bround expects exactly 2 arguments, got {}",
                args.len()
            );
        }
        let [x, d] = args.as_slice() else {
            return exec_err!("bround() requires exactly 2 arguments, got {}", args.len());
        };
        match (x, d) {
            // Decimal128, Int32
            // Float64,    Float64
            // Float32,    Float64
            // Float64,    Int32
            // Float32,    Int32
            // Float32,    Int32
            // null Float64, Float32, Int32
            (
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(val), _precision, scale)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(d))),
            ) => {
                let float_val = *val as f64 / 10f64.powi(*scale as i32);
                let scale = 10f64.powi(*d);
                let rounded = (float_val * scale).round() / scale;
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(rounded))))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(x_val))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(d_val))),
            ) => {
                let scale: f64 = 10f64.powf(*d_val);
                let rounded: f64 = (x_val * scale).round() / scale;
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(rounded))))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Float32(Some(x_val))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(d_val))),
            ) => {
                let scale: f32 = 10f32.powi(*d_val as i32);
                let rounded: f32 = (x_val * scale).round() / scale;
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(rounded))))
            }

            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(x_val))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(d_val))),
            ) => {
                let scale = 10f64.powi(*d_val);
                let rounded = (x_val * scale).round() / scale;
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(rounded))))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Float32(Some(x_val))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(d_val))),
            ) => {
                let scale = 10f32.powi(*d_val);
                let rounded = (x_val * scale).round() / scale;
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(rounded))))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Float64(None)),
                _,
            ) | (
                _,
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))),
            (
                ColumnarValue::Scalar(ScalarValue::Float32(None)),
                _,
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(None))),

            other => exec_err!(
                "bround only supports scalar Float32 or Float64 with Int32 as scale, received {other:?}"
            ),
        }
    }
}
