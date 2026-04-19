use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, Float32Array, Float64Array};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion::arrow::util::display::FormatOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr_fn::cast;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::{div_ceil, div_floor};

use crate::error::{
    generic_exec_err, generic_internal_err, invalid_arg_count_exec_err,
    unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::scalar::math::utils::decimal::round_decimal_base;

/// Extract the `target_scale` for the optional 2nd argument of `ceil` / `floor`.
/// Arity and integer type are already enforced by `coerce_types`; this only verifies
/// the scalar Int32 literal shape.
fn extract_target_scale<'a>(name: &str, args: &'a [ColumnarValue]) -> Result<&'a Option<i32>> {
    if args.len() < 2 {
        return Ok(&None);
    }
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(value),
        other => Err(unsupported_data_type_exec_err(
            name,
            "Target scale must be Integer literal",
            &other.data_type(),
        )),
    }
}

fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        Ok(vec![ceil_floor_coerce_first_arg(name, &arg_types[0])?])
    } else if arg_types.len() == 2 {
        let first = ceil_floor_coerce_first_arg(name, &arg_types[0])?;
        if arg_types[1].is_integer() {
            Ok(vec![first, DataType::Int32])
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
    // Arity is enforced by `coerce_types` (1 or 2 args).
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
            // Decimal128 can hold 10^37 (38 digits: 1 followed by 37 zeros) but not 10^38.
            // target_scale = -n produces a result that's a multiple of 10^n, so n ≤ 37.
            if target_scale < -37 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than or equal to -37",
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
        // Unreachable: `coerce_types` enforces 1 or 2 args.
        Err(generic_exec_err(name, "unexpected arg count"))
    }?;
    Ok(Arc::new(Field::new(name.to_string(), return_type, true)))
}

fn ceil_floor_coerce_first_arg(name: &str, arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Null => Ok(DataType::Float64),
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
        other if other.is_numeric() => Ok(other.clone()),
        other => Err(unsupported_data_type_exec_err(
            name,
            "First arg must be Numeric Type",
            other,
        )),
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
    fn as_any(&self) -> &dyn Any {
        self
    }
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Arity is enforced by `coerce_types` (1 or 2 args).
        let target_scale = extract_target_scale("ceil", &args.args)?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("ceil", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("ceil", arg_types)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        simplify_ceil_floor("spark_ceil", args, info)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        propagate_ceil_floor(CeilFloor::Ceil, interval, inputs)
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
    fn as_any(&self) -> &dyn Any {
        self
    }
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Arity is enforced by `coerce_types` (1 or 2 args).
        let target_scale = extract_target_scale("floor", &args.args)?;
        let arg = &args.args[0];
        let return_type = args.return_field.data_type();
        spark_ceil_floor("floor", arg, target_scale, return_type)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("floor", arg_types)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        simplify_ceil_floor("spark_floor", args, info)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        propagate_ceil_floor(CeilFloor::Floor, interval, inputs)
    }
}

#[derive(Clone, Copy)]
enum CeilFloor {
    Ceil,
    Floor,
}

const PROPAGATE_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

/// Constraint propagation for `ceil` / `floor` (Rule 17): given a known output `interval`
/// (e.g. from `WHERE ceil(col) > 5`), derive the widest safe interval on the input that can
/// still produce that output. Enables filter pushdown past the function.
///
/// Math (both functions are monotonic non-decreasing):
/// - `ceil(x) ∈ [a, b]`  ⇔  `x ∈ (a - 1, b]`   → safe over-approximation: `[a - 1, b]`
/// - `floor(x) ∈ [a, b]` ⇔  `x ∈ [a, b + 1)`  → safe over-approximation: `[a, b + 1]`
///
/// Over-approximating is sound: the final filter re-evaluates `ceil/floor(x)` so the extra
/// rows are discarded; missing rows would be a correctness bug. The result is intersected
/// with the current child interval to keep any tighter pre-existing bounds.
fn propagate_ceil_floor(
    kind: CeilFloor,
    interval: &Interval,
    inputs: &[&Interval],
) -> Result<Option<Vec<Interval>>> {
    let Some(child) = inputs.first() else {
        return Ok(Some(vec![]));
    };
    let child_type = child.data_type();
    let cast_output = interval.cast_to(&child_type, &PROPAGATE_CAST_OPTIONS)?;
    let one = ScalarValue::new_one(&child_type)?;
    let (lower, upper) = match kind {
        CeilFloor::Ceil => (cast_output.lower().sub(&one)?, cast_output.upper().clone()),
        CeilFloor::Floor => (cast_output.lower().clone(), cast_output.upper().add(&one)?),
    };
    let widened = Interval::try_new(lower, upper)?;
    Ok(child.intersect(widened)?.map(|refined| vec![refined]))
}

/// Algebraic simplifications for `ceil` and `floor`:
/// - `f(f(x)) = f(x)` — idempotent (same-function nesting).
/// - `f(int_expr) = cast(int_expr AS BIGINT)` (1-arg only) — no rounding on integers.
fn simplify_ceil_floor(
    self_name: &str,
    args: Vec<Expr>,
    info: &SimplifyContext,
) -> Result<ExprSimplifyResult> {
    if args.len() != 1 {
        return Ok(ExprSimplifyResult::Original(args));
    }
    let arg = &args[0];
    if let Expr::ScalarFunction(inner) = arg {
        if inner.func.name() == self_name {
            return Ok(ExprSimplifyResult::Simplified(arg.clone()));
        }
    }
    if info.get_data_type(arg)?.is_integer() {
        return Ok(ExprSimplifyResult::Simplified(cast(
            arg.clone(),
            DataType::Int64,
        )));
    }
    Ok(ExprSimplifyResult::Original(args))
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
                    let masked = mask_non_finite_f32(arg);
                    let arg = masked.cast_to(
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
                    let masked = mask_non_finite_f64(arg);
                    let arg = masked.cast_to(
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

/// Replace NaN and +/-Infinity values with NULL so they survive the cast to Decimal128.
/// Spark semantics: `ceil(NaN, s)` and `ceil(+/-Infinity, s)` return NULL even under ANSI,
/// because NaN/Inf have no valid decimal representation. Finite overflow is intentionally
/// left to raise a cast error (matching Spark ANSI=true).
fn mask_non_finite_f32(arg: &ColumnarValue) -> ColumnarValue {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) if !v.is_finite() => {
            ColumnarValue::Scalar(ScalarValue::Float32(None))
        }
        ColumnarValue::Array(array) => {
            let float_arr = array.as_primitive::<Float32Type>();
            let masked: Float32Array = float_arr
                .iter()
                .map(|v| match v {
                    Some(x) if !x.is_finite() => None,
                    other => other,
                })
                .collect();
            ColumnarValue::Array(Arc::new(masked) as ArrayRef)
        }
        other => other.clone(),
    }
}

fn mask_non_finite_f64(arg: &ColumnarValue) -> ColumnarValue {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) if !v.is_finite() => {
            ColumnarValue::Scalar(ScalarValue::Float64(None))
        }
        ColumnarValue::Array(array) => {
            let float_arr = array.as_primitive::<Float64Type>();
            let masked: Float64Array = float_arr
                .iter()
                .map(|v| match v {
                    Some(x) if !x.is_finite() => None,
                    other => other,
                })
                .collect();
            ColumnarValue::Array(Arc::new(masked) as ArrayRef)
        }
        other => other.clone(),
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
            // target_scale >= input scale: no rounding needed. `round_decimal_base`
            // keeps the return scale equal to the input scale, so the stored value
            // is unchanged.
            decimal
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
