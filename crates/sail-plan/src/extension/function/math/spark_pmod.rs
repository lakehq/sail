use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{i256, DataType, Decimal128Type, Decimal256Type, Float64Type};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::extension::function::math::both_are_decimal;

#[derive(Debug)]
pub struct SparkPmod {
    signature: Signature,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPmod {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkPmod {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_pmod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err("pmod", (2, 2), arg_types.len()));
        }
        let (dividend, divisor) = (&arg_types[0], &arg_types[1]);
        if both_are_decimal(dividend, divisor) {
            Ok(divisor.clone())
        } else {
            Ok(DataType::Float64)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(invalid_arg_count_exec_err("pmod", (2, 2), args.args.len()));
        }
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(dividend)),
                ColumnarValue::Scalar(ScalarValue::Float64(divisor)),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0.0f64 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                        dividend.map(|dividend| pmod_f64(dividend, divisor)),
                    )))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    dividend,
                    _dividend_precision,
                    _dividend_scale,
                )),
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    divisor,
                    divisor_precision,
                    divisor_scale,
                )),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0i128 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        dividend.map(|dividend| pmod_i128(dividend, divisor)),
                        *divisor_precision,
                        *divisor_scale,
                    )))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        None,
                        *divisor_precision,
                        *divisor_scale,
                    )))
                }
            }
            (
                ColumnarValue::Scalar(ScalarValue::Decimal256(
                    dividend,
                    _dividend_precision,
                    _dividend_scale,
                )),
                ColumnarValue::Scalar(ScalarValue::Decimal256(
                    divisor,
                    divisor_precision,
                    divisor_scale,
                )),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == i256::ZERO {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        dividend.map(|dividend| pmod_i256(dividend, divisor)),
                        *divisor_precision,
                        *divisor_scale,
                    )))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        None,
                        *divisor_precision,
                        *divisor_scale,
                    )))
                }
            }
            (
                ColumnarValue::Array(dividend_array),
                ColumnarValue::Scalar(ScalarValue::Float64(divisor)),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0.0f64 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    match dividend_array.data_type() {
                        DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                            dividend_array
                                .as_primitive::<Float64Type>()
                                .unary::<_, Float64Type>(|dividend| pmod_f64(dividend, divisor)),
                        )
                            as ArrayRef)),
                        other => Err(unsupported_data_type_exec_err(
                            "pmod",
                            format!("{}", DataType::Float64).as_str(),
                            other,
                        )),
                    }
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
                }
            }
            (
                ColumnarValue::Array(dividend_array),
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    divisor,
                    divisor_precision,
                    divisor_scale,
                )),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0i128 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    match dividend_array.data_type() {
                        DataType::Decimal128(_, _) => Ok(ColumnarValue::Array(Arc::new(
                            dividend_array
                                .as_primitive::<Decimal128Type>()
                                .unary::<_, Decimal128Type>(|dividend| {
                                    pmod_i128(dividend, divisor)
                                }),
                        )
                            as ArrayRef)),
                        other => Err(unsupported_data_type_exec_err(
                            "pmod",
                            format!(
                                "{}",
                                DataType::Decimal128(*divisor_precision, *divisor_scale)
                            )
                            .as_str(),
                            other,
                        )),
                    }
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        None,
                        *divisor_precision,
                        *divisor_scale,
                    )))
                }
            }
            (
                ColumnarValue::Array(dividend_array),
                ColumnarValue::Scalar(ScalarValue::Decimal256(
                    divisor,
                    divisor_precision,
                    divisor_scale,
                )),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == i256::ZERO {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    match dividend_array.data_type() {
                        DataType::Decimal256(_, _) => Ok(ColumnarValue::Array(Arc::new(
                            dividend_array
                                .as_primitive::<Decimal256Type>()
                                .unary::<_, Decimal256Type>(|dividend| {
                                    pmod_i256(dividend, divisor)
                                }),
                        )
                            as ArrayRef)),
                        other => Err(unsupported_data_type_exec_err(
                            "pmod",
                            format!(
                                "{}",
                                DataType::Decimal256(*divisor_precision, *divisor_scale)
                            )
                            .as_str(),
                            other,
                        )),
                    }
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        None,
                        *divisor_precision,
                        *divisor_scale,
                    )))
                }
            }
            (ColumnarValue::Array(dividend_array), ColumnarValue::Array(divisor_array)) => {
                match (dividend_array.data_type(), divisor_array.data_type()) {
                    (DataType::Float64, DataType::Float64) => {
                        let dividend_array = dividend_array.as_primitive::<Float64Type>();
                        let divisor_array = divisor_array.as_primitive::<Float64Type>();
                        Ok(ColumnarValue::Array(
                            Arc::new(try_binary::<_, _, _, Float64Type>(
                                dividend_array,
                                divisor_array,
                                |dividend, divisor| {
                                    if divisor == 0.0f64 {
                                        Err(ArrowError::DivideByZero)
                                    } else {
                                        Ok(pmod_f64(dividend, divisor))
                                    }
                                },
                            )?) as ArrayRef,
                        ))
                    }
                    (
                        DataType::Decimal128(_, _),
                        DataType::Decimal128(divisor_precision, divisor_scale),
                    ) => {
                        let dividend_array = dividend_array.as_primitive::<Decimal128Type>();
                        let divisor_array = divisor_array.as_primitive::<Decimal128Type>();
                        Ok(ColumnarValue::Array(Arc::new(
                            try_binary::<_, _, _, Decimal128Type>(
                                dividend_array,
                                divisor_array,
                                |dividend, divisor| {
                                    if divisor == 0i128 {
                                        Err(ArrowError::DivideByZero)
                                    } else {
                                        Ok(pmod_i128(dividend, divisor))
                                    }
                                },
                            )?
                            .with_data_type(DataType::Decimal128(
                                *divisor_precision,
                                *divisor_scale,
                            )),
                        ) as ArrayRef))
                    }
                    (
                        DataType::Decimal256(_, _),
                        DataType::Decimal256(divisor_precision, divisor_scale),
                    ) => {
                        let dividend_array = dividend_array.as_primitive::<Decimal256Type>();
                        let divisor_array = divisor_array.as_primitive::<Decimal256Type>();
                        Ok(ColumnarValue::Array(Arc::new(
                            try_binary::<_, _, _, Decimal256Type>(
                                dividend_array,
                                divisor_array,
                                |dividend, divisor| {
                                    if divisor == i256::ZERO {
                                        Err(ArrowError::DivideByZero)
                                    } else {
                                        Ok(pmod_i256(dividend, divisor))
                                    }
                                },
                            )?
                            .with_data_type(DataType::Decimal256(
                                *divisor_precision,
                                *divisor_scale,
                            )),
                        ) as ArrayRef))
                    }
                    (_dividend, _divisor) => Err(unsupported_data_types_exec_err(
                        "pmod",
                        "Numeric Type",
                        &[args.args[0].data_type(), args.args[1].data_type()],
                    )),
                }
            }
            (dividend, divisor) => Err(unsupported_data_types_exec_err(
                "pmod",
                "Numeric Type",
                &[dividend.data_type(), divisor.data_type()],
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err("pmod", (2, 2), arg_types.len()));
        }
        let (dividend, divisor) = (&arg_types[0], &arg_types[1]);
        if both_are_decimal(dividend, divisor) {
            Ok(vec![divisor.clone(), divisor.clone()])
        } else if dividend.is_numeric() && divisor.is_numeric() {
            Ok(vec![DataType::Float64, DataType::Float64])
        } else {
            Err(unsupported_data_types_exec_err(
                "pmod",
                "Numeric Type",
                arg_types,
            ))
        }
    }
}

fn pmod_f64(dividend: f64, divisor: f64) -> f64 {
    if divisor.is_sign_negative() {
        dividend % divisor
    } else {
        dividend.rem_euclid(divisor)
    }
}

fn pmod_i128(dividend: i128, divisor: i128) -> i128 {
    if divisor.is_negative() {
        dividend % divisor
    } else {
        dividend.rem_euclid(divisor)
    }
}

fn pmod_i256(dividend: i256, divisor: i256) -> i256 {
    if divisor.is_negative() {
        dividend % divisor
    } else {
        let r = dividend % divisor;
        if r < i256::ZERO {
            r.wrapping_add(divisor.wrapping_abs())
        } else {
            r
        }
    }
}
