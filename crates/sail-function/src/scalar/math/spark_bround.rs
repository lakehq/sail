use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    as_primitive_array, Array, Float32Array, Float64Array, Int32Array, Int64Array,
};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
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
            signature: Signature::variadic_any(Volatility::Immutable),
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
        if !(1..=2).contains(&arg_types.len()) {
            return Err(invalid_arg_count_exec_err(
                "spark_bround",
                (1, 2),
                arg_types.len(),
            ));
        }
        let t = &arg_types[0];
        match t {
            DataType::Float64
            | DataType::Float32
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => Ok(DataType::Float64),
            DataType::Int32 => Ok(DataType::Int32),
            DataType::Int64 => Ok(DataType::Int64),
            _ => Err(unsupported_data_type_exec_err(
                "spark_bround",
                "Float64, Float32, Decimal128, Decimal256, Int32, Int64",
                t,
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let args: Vec<ColumnarValue> = match args.len() {
            1 => vec![
                args[0].clone(),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
            ],
            2 => args.to_vec(),
            _ => {
                return Err(invalid_arg_count_exec_err(
                    "spark_bround",
                    (1, 2),
                    args.len(),
                ))
            }
        };
        let [x, d] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_bround",
                (2, 2),
                args.len(),
            ));
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

        match (x_array.data_type(), d_array.data_type()) {
            (DataType::Float64, DataType::Int32) => {
                let x = as_primitive_array::<Float64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Float64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => Some(spark_bround_f64(x, s)),
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
                            Some(round_half_to_even_f32(x * pow) / pow)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int32, DataType::Int32) => {
                let x = as_primitive_array::<Int32Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Int32Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let x_f64 = x as f64;
                            let pow = 10f64.powi(s);
                            Some((round_half_to_even_f64(x_f64 * pow) / pow) as i32)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            (DataType::Int64, DataType::Int32) => {
                let x = as_primitive_array::<Int64Type>(&x_array);
                let d = as_primitive_array::<Int32Type>(&d_array);
                let result: Int64Array = x
                    .iter()
                    .zip(d.iter())
                    .map(|(x_val, scale)| match (x_val, scale) {
                        (Some(x), Some(s)) => {
                            let x_f64 = x as f64;
                            let pow = 10f64.powi(s);
                            Some((round_half_to_even_f64(x_f64 * pow).round() / pow).round() as i64)
                        }
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // fallback
            (DataType::Decimal128(_, _), DataType::Int32) => Err(unsupported_data_types_exec_err(
                "spark_bround",
                "vectorized Decimal128",
                &[x_array.data_type().clone(), d_array.data_type().clone()],
            )),

            (dt_x, dt_d) => Err(unsupported_data_types_exec_err(
                "spark_bround",
                "Float32 | Float64 | Int32 | Int64 with Int32/Float64",
                &[dt_x.clone(), dt_d.clone()],
            )),
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        // FIXME check if need delete coerce
        match types.len() {
            1 => {
                let x_type: &DataType = &types[0];
                let valid_x: bool = matches!(
                    x_type,
                    DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                        | DataType::Int32
                        | DataType::Int64
                );

                if valid_x {
                    Ok(vec![x_type.clone(), DataType::Int32])
                } else {
                    Err(unsupported_data_types_exec_err(
                        "spark_bround",
                        "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64",
                        types,
                    ))
                }
            }
            2 => {
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
                    Err(unsupported_data_types_exec_err(
                        "spark_bround",
                        "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64, Int32",
                        types,
                    ))
                }
            }
            _ => Err(invalid_arg_count_exec_err(
                "spark_bround",
                (1, 2),
                types.len(),
            )),
        }
    }
}

fn round_half_to_even_f64(value: f64) -> f64 {
    let rounded: f64 = value.trunc();
    let fraction: f64 = value - rounded;

    if fraction.abs() != 0.5 {
        return value.round();
    }

    if (rounded as i64) % 2 == 0 {
        rounded
    } else if value > 0.0 {
        rounded + 1.0
    } else {
        rounded - 1.0
    }
}

fn round_half_to_even_f32(value: f32) -> f32 {
    let rounded: f32 = value.trunc();
    let fraction: f32 = value - rounded;

    if fraction.abs() != 0.5 {
        return value.round();
    }

    if (rounded as i32) % 2 == 0 {
        rounded
    } else if value > 0.0 {
        rounded + 1.0
    } else {
        rounded - 1.0
    }
}

fn spark_bround_f64(x: f64, scale: i32) -> f64 {
    let factor: f64 = 10f64.powi(scale);
    let shifted: f64 = x * factor;

    let rounded = if (shifted - shifted.round()).abs() == 0.5 {
        round_half_to_even_f64(shifted)
    } else {
        shifted.round()
    };

    rounded / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bround_f64_positive_scale() {
        assert_eq!(spark_bround_f64(1.25, 1), 1.2);
        assert_eq!(spark_bround_f64(1.35, 1), 1.4);
    }
    #[test]
    fn test_bround_f64_zero_scale() {
        assert_eq!(spark_bround_f64(2.5, 0), 2.0);
        assert_eq!(spark_bround_f64(3.5, 0), 4.0);
        assert_eq!(spark_bround_f64(-2.5, 0), -2.0);
        assert_eq!(spark_bround_f64(-3.5, 0), -4.0);
    }
    #[test]
    fn test_bround_f64_negative_scale() {
        assert_eq!(spark_bround_f64(25.0, -1), 20.0);
        assert_eq!(spark_bround_f64(35.0, -1), 40.0);
        assert_eq!(spark_bround_f64(-25.0, -1), -20.0);
        assert_eq!(spark_bround_f64(-35.0, -1), -40.0);
    }
    #[test]
    fn test_bround_no_tie() {
        assert_eq!(spark_bround_f64(2.4, 0), 2.0);
        assert_eq!(spark_bround_f64(2.6, 0), 3.0);
        assert_eq!(spark_bround_f64(-2.4, 0), -2.0);
        assert_eq!(spark_bround_f64(-2.6, 0), -3.0);
    }
}
