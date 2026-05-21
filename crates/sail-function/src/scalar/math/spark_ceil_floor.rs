use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    expr, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use num::integer::{div_ceil, div_floor};
use num::traits::CheckedAdd;

use crate::error::{
    generic_exec_err, generic_internal_err, invalid_arg_count_exec_err,
    unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::scalar::math::utils::decimal::round_decimal_base;

fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        if arg_types[0].is_numeric() {
            Ok(vec![ceil_floor_coerce_first_arg(name, &arg_types[0])?])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type",
                arg_types,
            ))
        }
    } else if arg_types.len() == 2 {
        if arg_types[0].is_numeric() && arg_types[1].is_integer() {
            Ok(vec![
                ceil_floor_coerce_first_arg(name, &arg_types[0])?,
                DataType::Int32,
            ])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type for expr and Integer Type for target scale",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()))
    }
}

fn ceil_floor_return_type_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_fields = args.arg_fields;
    let scalar_arguments = args.scalar_arguments;
    let return_type = if arg_fields.len() == 1 {
        match &arg_fields[0].data_type() {
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, true);
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    let (precision, scale) =
                        round_decimal_base(*precision as i32, *scale as i32, 0, false);
                    Ok(DataType::Decimal128(precision, scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        arg_fields[0].data_type(),
                    ))
                }
            }
            _ => Ok(DataType::Int64),
        }
    } else if arg_fields.len() == 2 {
        if let Some(target_scale) = scalar_arguments[1] {
            let expr = &arg_fields[0].data_type();
            let target_scale: i32 = match target_scale {
                ScalarValue::Int8(Some(v)) => Ok(*v as i32),
                ScalarValue::Int16(Some(v)) => Ok(*v as i32),
                ScalarValue::Int32(Some(v)) => Ok(*v),
                ScalarValue::Int64(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt8(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt16(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt32(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt64(Some(v)) => Ok(*v as i32),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }?;
            if target_scale < -38 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than -38",
                ));
            }
            let (precision, scale) = match expr {
                DataType::Int8 => Ok((3, 0)),
                DataType::UInt8 | DataType::Int16 => Ok((5, 0)),
                DataType::UInt16 | DataType::Int32 => Ok((10, 0)),
                DataType::UInt32 | DataType::UInt64 | DataType::Int64 => Ok((20, 0)),
                DataType::Float32 => Ok((14, 7)),
                DataType::Float64 => Ok((30, 15)),
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                        Ok((*precision as i32, *scale as i32))
                    } else {
                        Err(unsupported_data_type_exec_err(
                            name,
                            format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                            arg_fields[0].data_type(),
                        ))
                    }
                }
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Numeric Type for expr",
                    expr,
                )),
            }?;
            let (precision, scale) = round_decimal_base(precision, scale, target_scale, true);
            Ok(DataType::Decimal128(precision, scale))
        } else {
            Err(generic_exec_err(
                name,
                "Target scale must be Integer literal, received: None",
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_fields.len()))
    }?;
    Ok(Arc::new(Field::new(name.to_string(), return_type, true)))
}

fn ceil_floor_coerce_first_arg(name: &str, arg_type: &DataType) -> Result<DataType> {
    if arg_type.is_numeric() {
        match arg_type {
            DataType::UInt8 => Ok(DataType::Int16),
            DataType::UInt16 => Ok(DataType::Int32),
            DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    Ok(DataType::Decimal128(*precision, *scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        arg_type,
                    ))
                }
            }
            other => Ok(other.clone()),
        }
    } else {
        Err(unsupported_data_type_exec_err(
            name,
            "First arg must be Numeric Type",
            arg_type,
        ))
    }
}

#[inline]
fn get_return_type_precision_scale(return_type: &DataType) -> Result<(u8, i8)> {
    match return_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        other => Err(generic_internal_err(
            "ceil",
            format!("Expected return type to be Decimal128, got: {other}").as_str(),
        )),
    }
}

fn ceil_floor_simplify<T: ScalarUDFImpl + 'static>(
    mut args: Vec<Expr>,
    info: &SimplifyContext,
) -> Result<ExprSimplifyResult> {
    // Only simplify the 1-arg form; 2-arg (with scale) is a genuine rounding op.
    if args.len() != 1 {
        return Ok(ExprSimplifyResult::Original(args));
    }
    let arg = args.remove(0);
    // Integer identity: ceil/floor of an integer is the integer itself.
    // The 1-arg return type is always Int64, so narrow types need a cast.
    match info.get_data_type(&arg)? {
        DataType::Int64 => return Ok(ExprSimplifyResult::Simplified(arg)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            return Ok(ExprSimplifyResult::Simplified(Expr::Cast(expr::Cast::new(
                Box::new(arg),
                DataType::Int64,
            ))));
        }
        _ => {}
    }
    // Idempotence: floor(floor(x)) = floor(x), ceil(ceil(x)) = ceil(x).
    if let Expr::ScalarFunction(ref func) = arg {
        if func.func.inner().downcast_ref::<T>().is_some() && func.args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(arg));
        }
    }
    Ok(ExprSimplifyResult::Original(vec![arg]))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn name(&self) -> &str {
        "spark_ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "ceil",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        ceil_floor_return_type_from_args("ceil", args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        ceil_floor_simplify::<Self>(args, info)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_len = args.args.len();
        let target_scale = if arg_len == 1 {
            Ok(&None)
        } else if arg_len == 2 {
            let target_scale = &args.args[1];
            match target_scale {
                ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(value),
                _ => Err(unsupported_data_type_exec_err(
                    "ceil",
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }
        } else {
            Err(invalid_arg_count_exec_err("ceil", (1, 2), arg_len))
        }?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("ceil", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("ceil", arg_types)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFloor {
    signature: Signature,
}

impl Default for SparkFloor {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFloor {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFloor {
    fn name(&self) -> &str {
        "spark_floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "floor",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        ceil_floor_return_type_from_args("floor", args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        ceil_floor_simplify::<Self>(args, info)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_len = args.args.len();
        let target_scale = if arg_len == 1 {
            Ok(&None)
        } else if arg_len == 2 {
            let target_scale = &args.args[1];
            match target_scale {
                ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(value),
                _ => Err(unsupported_data_type_exec_err(
                    "floor",
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }
        } else {
            Err(invalid_arg_count_exec_err("floor", (1, 2), arg_len))
        }?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("floor", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("floor", arg_types)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        preimage_floor(args, lit_expr, info)
    }
}

fn spark_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    if matches!(
        arg.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        if let Some(target_scale) = *target_scale {
            if target_scale >= 0 {
                Ok(arg.cast_to(return_type, None)?)
            } else {
                let (return_type_precision, return_type_scale) =
                    get_return_type_precision_scale(return_type)?;
                match arg.data_type() {
                    DataType::Int8 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int8(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int8Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int8).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int16 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int16(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int16Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int16).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int32 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int32Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int32).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int64 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int64Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int64).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    other => Err(unsupported_data_type_exec_err(
                        name,
                        "Numeric Type for expr",
                        &other,
                    )),
                }
            }
        } else {
            Ok(arg.cast_to(&DataType::Int64, None)?)
        }
    } else {
        match arg.data_type() {
            DataType::Float32 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let arg = arg.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        None,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f32::ceil
                    } else {
                        f32::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float32(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Float32Type>()
                                .unary::<_, Int64Type>(|x| func(x) as i64),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float32).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Float64 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let arg = arg.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        None,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f64::ceil
                    } else {
                        f64::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Float64Type>()
                                .unary::<_, Int64Type>(|x| func(x) as i64),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float32).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Decimal128(_precision, _scale) => {
                decimal128_ceil_floor(name, arg, target_scale, return_type)
            }
            other => Err(unsupported_data_type_exec_err(
                name,
                "Numeric Type for expr",
                &other,
            )),
        }
    }
}

fn decimal128_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    match arg.data_type() {
        DataType::Decimal128(_precision, scale) => {
            let (return_type_precision, return_type_scale) =
                get_return_type_precision_scale(return_type)?;
            let target_scale = (*target_scale).unwrap_or(0);
            match arg {
                ColumnarValue::Scalar(ScalarValue::Decimal128(value, _precision, _scale)) => {
                    if let Some(value) = value {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            Some(ceil_floor_with_target_scale(
                                name,
                                *value,
                                scale,
                                target_scale,
                            )),
                            return_type_precision,
                            return_type_scale,
                        )))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            None,
                            return_type_precision,
                            return_type_scale,
                        )))
                    }
                }
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Decimal128Type>()
                        .unary::<_, Decimal128Type>(|value| {
                            ceil_floor_with_target_scale(name, value, scale, target_scale)
                        })
                        .with_data_type(DataType::Decimal128(
                            return_type_precision,
                            return_type_scale,
                        )),
                )
                    as ArrayRef)),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Decimal128 Type",
                    &arg.data_type(),
                )),
            }
        }
        other => Err(unsupported_data_type_exec_err(
            name,
            "Decimal128 Type for Decimal128 ceil",
            &other,
        )),
    }
}

#[inline]
fn ceil_floor_with_target_scale(name: &str, decimal: i128, scale: i8, target_scale: i32) -> i128 {
    // Round to powers of 10 to the left of decimal point when target_scale < 0
    if target_scale < 0 {
        // Convert to integer with scale 0
        let integer_value = match scale.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let factor = 10_i128.pow_wrapping(scale as u32);
                if matches!(name, "ceil") {
                    div_ceil(decimal, factor)
                } else {
                    div_floor(decimal, factor)
                }
            }
            std::cmp::Ordering::Less => decimal * 10_i128.pow_wrapping((-scale) as u32),
            std::cmp::Ordering::Equal => decimal,
        };
        let pow_factor = 10_i128.pow_wrapping((-target_scale) as u32);
        if matches!(name, "ceil") {
            div_ceil(integer_value, pow_factor) * pow_factor
        } else {
            div_floor(integer_value, pow_factor) * pow_factor
        }
    } else {
        let scale_diff = target_scale - (scale as i32);
        if scale_diff >= 0 {
            decimal * 10_i128.pow_wrapping(scale_diff as u32)
        } else {
            let abs_diff = (-scale_diff) as u32;
            if matches!(name, "ceil") {
                div_ceil(decimal, 10_i128.pow_wrapping(abs_diff))
            } else {
                div_floor(decimal, 10_i128.pow_wrapping(abs_diff))
            }
        }
    }
}

/// Preimage for `floor(x) = N`: the set of `x` with `floor(x) = N` is exactly
/// the half-open interval `[N, N + 1)`, which matches `PreimageResult::Range`.
/// Enables the logical simplifier to rewrite `WHERE floor(col) = N` into
/// `WHERE col >= N AND col < N + 1`, dropping the UDF call and unlocking
/// downstream pruning (min/max stats, `PruningPredicate`).
///
/// Only the 1-arg form is handled. Returns `PreimageResult::None` when the
/// rewrite would be unsound: non-integer literal, NaN/Infinity, overflow of
/// `N + 1`, or unsupported literal type.
fn preimage_floor(
    args: &[Expr],
    lit_expr: &Expr,
    info: &SimplifyContext,
) -> Result<PreimageResult> {
    if args.len() != 1 {
        return Ok(PreimageResult::None);
    }
    let Expr::Literal(lit_value, _) = lit_expr else {
        return Ok(PreimageResult::None);
    };
    // `floor` returns integer-valued results, so a non-integer RHS means the
    // equality is unsatisfiable.
    let Some(rhs) = lit_as_integer(lit_value) else {
        return Ok(PreimageResult::None);
    };
    let input_type = info.get_data_type(&args[0])?;

    let bounds = match &input_type {
        DataType::Float64 => float_bounds(rhs).map(|(lo, hi)| {
            (
                ScalarValue::Float64(Some(lo)),
                ScalarValue::Float64(Some(hi)),
            )
        }),
        DataType::Float32 => float_bounds(rhs).and_then(|(lo, hi)| {
            let lo32 = lo as f32;
            let hi32 = hi as f32;
            // f32 can only represent integers exactly up to 2^24; reject when
            // the round-trip would collapse the interval.
            if (lo32 as f64) != lo || (hi32 as f64) != hi || hi32 <= lo32 {
                None
            } else {
                Some((
                    ScalarValue::Float32(Some(lo32)),
                    ScalarValue::Float32(Some(hi32)),
                ))
            }
        }),
        DataType::Int8 => int_bounds::<i8>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int8(Some(lo)), ScalarValue::Int8(Some(hi)))),
        DataType::Int16 => int_bounds::<i16>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int16(Some(lo)), ScalarValue::Int16(Some(hi)))),
        DataType::Int32 => int_bounds::<i32>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int32(Some(lo)), ScalarValue::Int32(Some(hi)))),
        DataType::Int64 => int_bounds::<i64>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi)))),
        DataType::Decimal128(precision, scale) => decimal128_bounds(rhs, *precision, *scale),
        _ => None,
    };

    let Some((lower, upper)) = bounds else {
        return Ok(PreimageResult::None);
    };
    Ok(PreimageResult::Range {
        expr: args[0].clone(),
        interval: Box::new(Interval::try_new(lower, upper)?),
    })
}

/// Extract a literal RHS as an integer. Returns `None` for non-integer values
/// (fractional floats, NULL, strings, etc.).
fn lit_as_integer(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::Int8(Some(n)) => Some(*n as i128),
        ScalarValue::Int16(Some(n)) => Some(*n as i128),
        ScalarValue::Int32(Some(n)) => Some(*n as i128),
        ScalarValue::Int64(Some(n)) => Some(*n as i128),
        ScalarValue::Float32(Some(n)) if n.is_finite() && n.fract() == 0.0 => {
            // Round-trip check ensures the cast is lossless (guards against saturation
            // for floats outside i128 range and precision loss for |n| > 2^24).
            let i = *n as i128;
            (i as f32 == *n).then_some(i)
        }
        ScalarValue::Float64(Some(n)) if n.is_finite() && n.fract() == 0.0 => {
            let i = *n as i128;
            (i as f64 == *n).then_some(i)
        }
        ScalarValue::Decimal128(Some(n), _, 0) => Some(*n),
        ScalarValue::Decimal128(Some(n), _, scale) if *scale > 0 => {
            let pow = 10_i128.checked_pow(*scale as u32)?;
            if n % pow == 0 {
                Some(n / pow)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn float_bounds(n: i128) -> Option<(f64, f64)> {
    let lo = n as f64;
    let hi = n.checked_add(1).map(|m| m as f64)?;
    // Verify round-trip: if lo rounds (|n| > 2^53), the interval is unsound because
    // the float representation of lo differs from the integer n.
    if lo as i128 != n || !lo.is_finite() || !hi.is_finite() || hi <= lo {
        return None;
    }
    Some((lo, hi))
}

fn int_bounds<T>(n: i128) -> Option<(T, T)>
where
    T: TryFrom<i128> + CheckedAdd + num::One,
{
    let lo = T::try_from(n).ok()?;
    let hi = lo.checked_add(&T::one())?;
    Some((lo, hi))
}

fn decimal128_bounds(n: i128, precision: u8, scale: i8) -> Option<(ScalarValue, ScalarValue)> {
    if scale < 0 {
        return None;
    }
    let step = 10_i128.checked_pow(scale as u32)?;
    let lo = n.checked_mul(step)?;
    let hi = lo.checked_add(step)?;
    // Validate both bounds fit within the declared precision. Use u128 because
    // 10^38 (max precision) exceeds i128::MAX but fits in u128.
    let max_unscaled = 10_u128.checked_pow(u32::from(precision))?;
    if lo.unsigned_abs() >= max_unscaled || hi.unsigned_abs() >= max_unscaled {
        return None;
    }
    Some((
        ScalarValue::Decimal128(Some(lo), precision, scale),
        ScalarValue::Decimal128(Some(hi), precision, scale),
    ))
}
