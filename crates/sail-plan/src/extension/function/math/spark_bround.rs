use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{as_primitive_array, Array, Float32Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type};
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
            | DataType::Decimal256(_, _)
            | DataType::Int32
            | DataType::Int64 => Ok(DataType::Float64),

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
        if let (
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(val), _, scale)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(d))),
        ) = (x, d)
        {
            let float_val = *val as f64 / 10f64.powi(*scale as i32);
            let scale = 10f64.powi(*d);
            let rounded = (float_val * scale).round() / scale;
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(rounded))));
        }

        let len = match (x, d) {
            (ColumnarValue::Array(arr), _) => arr.len(),
            (_, ColumnarValue::Array(arr)) => arr.len(),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
        };

        let x_array = match x {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        let d_array = match d {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
        };

        if x_array.len() != d_array.len() {
            return exec_err!("Arguments to bround must have the same length");
        }

        match (x_array.data_type(), d_array.data_type()) {
            (DataType::Float64, DataType::Int32) => {
                let x = as_primitive_array::<Float64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Float64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let pow = 10f64.powi(s);
                            Some((x * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Float32, DataType::Int32) => {
                let x = as_primitive_array::<Float32Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Float32Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let pow = 10f32.powi(s);
                            Some((x * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int32, DataType::Int32) => {
                let x = as_primitive_array::<Int32Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Float64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let x_f64 = x as f64;
                            let pow = 10f64.powi(s);
                            Some((x_f64 * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int64, DataType::Int32) => {
                let x = as_primitive_array::<Int64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Float64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let x_f64 = x as f64;
                            let pow = 10f64.powi(s);
                            Some((x_f64 * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Float32, DataType::Float64) => {
                let x = as_primitive_array::<Float32Type>(&x_array);
                let d = as_primitive_array::<Float64Type>(&d_array);
                let result: Float32Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let pow = 10f32.powf(s as f32);
                            Some((x * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Float64, DataType::Float64) => {
                let x = as_primitive_array::<Float64Type>(&x_array);
                let d = as_primitive_array::<Float64Type>(&d_array);
                let result: Float64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let pow = 10f64.powf(s);
                            Some((x * pow).round() / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // fallback
            (DataType::Decimal128(_, _), DataType::Int32) => {
                exec_err!("bround does not support vectorized Decimal128 yet")
            }

            (dt_x, dt_d) => exec_err!(
                "bround does not support argument types {:?} and {:?}",
                dt_x,
                dt_d
            ),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if types.len() != 2 {
            return exec_err!(
                "Function spark_bround expects 2 arguments, got {}",
                types.len()
            );
        }

        let x_type: &DataType = &types[0];
        let scale_type: &DataType = &types[1];

        let valid_x: bool = matches!(
            x_type,
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Int32
                | DataType::Int64
        );
        let valid_scale: bool = matches!(scale_type, DataType::Int32);

        if valid_x && valid_scale {
            Ok(vec![x_type.clone(), scale_type.clone()])
        } else {
            exec_err!(
                "Function spark_bround does not support argument types ({:?}, {:?})",
                x_type,
                scale_type
            )
        }
    }
}
