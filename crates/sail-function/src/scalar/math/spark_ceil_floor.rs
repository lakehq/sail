use std::any::Any;
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
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
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
            return Ok(ExprSimplifyResult::Simplified(Expr::Cast(expr::Cast {
                expr: Box::new(arg),
                data_type: DataType::Int64,
            })));
        }
        _ => {}
    }
    // Idempotence: floor(floor(x)) = floor(x), ceil(ceil(x)) = ceil(x).
    if let Expr::ScalarFunction(ref func) = arg {
        if func.func.inner().as_any().is::<T>() && func.args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(arg));
        }
    }
    Ok(ExprSimplifyResult::Original(vec![arg]))
}

fn f64_ceil_to_i64(f: f64) -> Option<i64> {
    let c = f.ceil();
    // i64::MAX (2^63-1) rounds up to 2^63 in f64, so use strict < to avoid
    // saturating cast for c == 2^63. i64::MIN (-2^63) is exact, so >= is fine.
    if c >= i64::MIN as f64 && c < (i64::MAX as f64) {
        Some(c as i64)
    } else {
        None
    }
}

fn f32_ceil_to_i64(f: f32) -> Option<i64> {
    f64_ceil_to_i64(f as f64)
}

fn f64_floor_to_i64(f: f64) -> Option<i64> {
    let fl = f.floor();
    if fl >= i64::MIN as f64 && fl < (i64::MAX as f64) {
        Some(fl as i64)
    } else {
        None
    }
}

fn f32_floor_to_i64(f: f32) -> Option<i64> {
    f64_floor_to_i64(f as f64)
}

fn scalar_to_i64_ceil(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Float64(Some(f)) if f.is_finite() => f64_ceil_to_i64(*f),
        ScalarValue::Float32(Some(f)) if f.is_finite() => f32_ceil_to_i64(*f),
        ScalarValue::Int8(Some(n)) => Some(*n as i64),
        ScalarValue::Int16(Some(n)) => Some(*n as i64),
        ScalarValue::Int32(Some(n)) => Some(*n as i64),
        ScalarValue::Int64(Some(n)) => Some(*n),
        _ => None,
    }
}

fn scalar_to_i64_floor(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Float64(Some(f)) if f.is_finite() => f64_floor_to_i64(*f),
        ScalarValue::Float32(Some(f)) if f.is_finite() => f32_floor_to_i64(*f),
        ScalarValue::Int8(Some(n)) => Some(*n as i64),
        ScalarValue::Int16(Some(n)) => Some(*n as i64),
        ScalarValue::Int32(Some(n)) => Some(*n as i64),
        ScalarValue::Int64(Some(n)) => Some(*n),
        _ => None,
    }
}

fn extract_tight_scale(interval: &Interval) -> Option<i32> {
    match (interval.lower(), interval.upper()) {
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) if a == b => Some(*a),
        _ => None,
    }
}

/// Compute the Decimal128 output type for the 2-arg form, mirroring the
/// `ceil_floor_return_type_from_args` logic so `evaluate_bounds` always
/// returns an interval whose type matches the function's declared return type.
fn decimal128_2arg_output_type(input_type: &DataType, target_scale: i32) -> DataType {
    let (in_p, in_s): (i32, i32) = match input_type {
        DataType::Decimal128(p, s) => (*p as i32, *s as i32),
        DataType::Float32 => (14, 7),
        DataType::Float64 => (30, 15),
        DataType::Int8 => (3, 0),
        DataType::Int16 => (5, 0),
        DataType::Int32 => (10, 0),
        DataType::Int64 => (20, 0),
        _ => (DECIMAL128_MAX_PRECISION as i32, 0),
    };
    let (out_p, out_s) = round_decimal_base(in_p, in_s, target_scale, true);
    DataType::Decimal128(out_p, out_s)
}

fn decimal128_evaluate_bounds(
    name: &str,
    value_interval: &Interval,
    target_scale: i32,
) -> Result<Interval> {
    let input_type = value_interval.data_type();
    let (in_p, in_s) = match input_type {
        DataType::Decimal128(p, s) => (p, s),
        // Non-Decimal128 value (Float/Int): execution casts to Decimal128 first,
        // so we can't compute tight bounds here. Return unbounded with the correct
        // Decimal128 output type (matches ceil_floor_return_type_from_args).
        _ => {
            return Interval::make_unbounded(&decimal128_2arg_output_type(
                &input_type,
                target_scale,
            ));
        }
    };
    let (out_p, out_s) = round_decimal_base(in_p as i32, in_s as i32, target_scale, true);
    let out_type = DataType::Decimal128(out_p, out_s);
    let extract = |sv: &ScalarValue| match sv {
        ScalarValue::Decimal128(Some(v), _, _) => Some(*v),
        _ => None,
    };
    match (
        extract(value_interval.lower()),
        extract(value_interval.upper()),
    ) {
        (Some(lo), Some(hi)) => {
            let lo_out = ceil_floor_with_target_scale(name, lo, in_s, target_scale);
            let hi_out = ceil_floor_with_target_scale(name, hi, in_s, target_scale);
            Interval::try_new(
                ScalarValue::Decimal128(Some(lo_out), out_p, out_s),
                ScalarValue::Decimal128(Some(hi_out), out_p, out_s),
            )
            .or_else(|_| Interval::make_unbounded(&out_type))
        }
        _ => Interval::make_unbounded(&out_type),
    }
}

fn ceil_floor_output_type(input_type: &DataType) -> DataType {
    match input_type {
        DataType::Decimal128(p, s) => {
            let (p2, s2) = round_decimal_base(*p as i32, *s as i32, 0, true);
            DataType::Decimal128(p2, s2)
        }
        _ => DataType::Int64,
    }
}

fn i64_to_input_scalar(n: i64, dtype: &DataType) -> Option<ScalarValue> {
    match dtype {
        DataType::Float64 => Some(ScalarValue::Float64(Some(n as f64))),
        DataType::Float32 => {
            let f = n as f32;
            (f as i64 == n).then_some(ScalarValue::Float32(Some(f)))
        }
        DataType::Int8 => i8::try_from(n).ok().map(|v| ScalarValue::Int8(Some(v))),
        DataType::Int16 => i16::try_from(n).ok().map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int32 => i32::try_from(n).ok().map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int64 => Some(ScalarValue::Int64(Some(n))),
        _ => None,
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

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let [arg] = input else {
            return Ok(SortProperties::Unordered);
        };
        // monotonically non-decreasing: ceil(x) <= ceil(y) when x <= y
        Ok(arg.sort_properties)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        match inputs {
            [input] => {
                let out_type = ceil_floor_output_type(&input.data_type());
                match (
                    scalar_to_i64_ceil(input.lower()),
                    scalar_to_i64_ceil(input.upper()),
                ) {
                    (Some(lo), Some(hi)) => Interval::try_new(
                        ScalarValue::Int64(Some(lo)),
                        ScalarValue::Int64(Some(hi)),
                    )
                    .or_else(|_| Interval::make_unbounded(&out_type)),
                    _ => Interval::make_unbounded(&out_type),
                }
            }
            [value_interval, scale_interval] => {
                if let Some(target_scale) = extract_tight_scale(scale_interval) {
                    decimal128_evaluate_bounds("ceil", value_interval, target_scale)
                } else {
                    // Non-tight scale: can't narrow. 2-arg always returns Decimal128;
                    // use target_scale=0 as a conservative type approximation.
                    Interval::make_unbounded(&decimal128_2arg_output_type(
                        &value_interval.data_type(),
                        0,
                    ))
                }
            }
            _ => Interval::make_unbounded(&DataType::Int64),
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [input_interval] = inputs else {
            return Ok(Some(inputs.iter().map(|i| (*i).clone()).collect()));
        };
        let input_type = input_interval.data_type();
        // ceil(x) ∈ [N, M] → x ∈ (N-1, M] — use [N-1, M] (conservative closed)
        let lo = if let ScalarValue::Int64(Some(n)) = interval.lower() {
            n.checked_sub(1)
                .and_then(|n1| i64_to_input_scalar(n1, &input_type))
        } else {
            None
        };
        let hi = if let ScalarValue::Int64(Some(n)) = interval.upper() {
            i64_to_input_scalar(*n, &input_type)
        } else {
            None
        };
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                let constraint = Interval::try_new(lo, hi)?;
                Ok(input_interval.intersect(constraint)?.map(|r| vec![r]))
            }
            _ => Ok(Some(vec![(**input_interval).clone()])),
        }
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

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let [arg] = input else {
            return Ok(SortProperties::Unordered);
        };
        // monotonically non-decreasing: floor(x) <= floor(y) when x <= y
        Ok(arg.sort_properties)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        match inputs {
            [input] => {
                let out_type = ceil_floor_output_type(&input.data_type());
                match (
                    scalar_to_i64_floor(input.lower()),
                    scalar_to_i64_floor(input.upper()),
                ) {
                    (Some(lo), Some(hi)) => Interval::try_new(
                        ScalarValue::Int64(Some(lo)),
                        ScalarValue::Int64(Some(hi)),
                    )
                    .or_else(|_| Interval::make_unbounded(&out_type)),
                    _ => Interval::make_unbounded(&out_type),
                }
            }
            [value_interval, scale_interval] => {
                if let Some(target_scale) = extract_tight_scale(scale_interval) {
                    decimal128_evaluate_bounds("floor", value_interval, target_scale)
                } else {
                    // Non-tight scale: can't narrow. 2-arg always returns Decimal128;
                    // use target_scale=0 as a conservative type approximation.
                    Interval::make_unbounded(&decimal128_2arg_output_type(
                        &value_interval.data_type(),
                        0,
                    ))
                }
            }
            _ => Interval::make_unbounded(&DataType::Int64),
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [input_interval] = inputs else {
            return Ok(Some(inputs.iter().map(|i| (*i).clone()).collect()));
        };
        let input_type = input_interval.data_type();
        // floor(x) ∈ [N, M] → x ∈ [N, M+1) — use [N, M+1] (conservative closed)
        let lo = if let ScalarValue::Int64(Some(n)) = interval.lower() {
            i64_to_input_scalar(*n, &input_type)
        } else {
            None
        };
        let hi = if let ScalarValue::Int64(Some(n)) = interval.upper() {
            i64::checked_add(*n, 1).and_then(|n1| i64_to_input_scalar(n1, &input_type))
        } else {
            None
        };
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                let constraint = Interval::try_new(lo, hi)?;
                Ok(input_interval.intersect(constraint)?.map(|r| vec![r]))
            }
            _ => Ok(Some(vec![(**input_interval).clone()])),
        }
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::compute::SortOptions;
    use datafusion_common::Result;

    use super::*;

    fn interval_f64(lo: f64, hi: f64) -> Result<Interval> {
        Interval::try_new(
            ScalarValue::Float64(Some(lo)),
            ScalarValue::Float64(Some(hi)),
        )
    }

    fn interval_i64(lo: i64, hi: i64) -> Result<Interval> {
        Interval::try_new(ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi)))
    }

    fn make_simplify_ctx(
        name: &str,
        dtype: DataType,
    ) -> Result<datafusion_expr::simplify::SimplifyContext> {
        use datafusion_common::DFSchema;
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![datafusion::arrow::datatypes::Field::new(name, dtype, false)].into(),
            Default::default(),
        )?);
        Ok(datafusion_expr::simplify::SimplifyContext::default().with_schema(schema))
    }

    // --- output_ordering ---

    #[test]
    fn test_ceil_output_ordering_ascending() -> Result<()> {
        let props =
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: false,
            }));
        let result = SparkCeil::new().output_ordering(std::slice::from_ref(&props))?;
        assert_eq!(result, props.sort_properties);
        Ok(())
    }

    #[test]
    fn test_ceil_output_ordering_descending() -> Result<()> {
        let props =
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }));
        let result = SparkCeil::new().output_ordering(std::slice::from_ref(&props))?;
        assert_eq!(result, props.sort_properties);
        Ok(())
    }

    #[test]
    fn test_floor_output_ordering_ascending() -> Result<()> {
        let props =
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: false,
            }));
        let result = SparkFloor::new().output_ordering(std::slice::from_ref(&props))?;
        assert_eq!(result, props.sort_properties);
        Ok(())
    }

    #[test]
    fn test_floor_output_ordering_descending() -> Result<()> {
        let props =
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }));
        let result = SparkFloor::new().output_ordering(std::slice::from_ref(&props))?;
        assert_eq!(result, props.sort_properties);
        Ok(())
    }

    #[test]
    fn test_ceil_output_ordering_unordered() -> Result<()> {
        let props = ExprProperties::new_unknown();
        let result = SparkCeil::new().output_ordering(std::slice::from_ref(&props))?;
        assert_eq!(result, SortProperties::Unordered);
        Ok(())
    }

    #[test]
    fn test_ceil_output_ordering_multiple_args_is_unordered() -> Result<()> {
        // 2-arg form: arity != 1 → Unordered
        let p1 = ExprProperties::new_unknown().with_order(SortProperties::Ordered(SortOptions {
            descending: false,
            nulls_first: false,
        }));
        let p2 = ExprProperties::new_unknown();
        let result = SparkCeil::new().output_ordering(&[p1, p2])?;
        assert_eq!(result, SortProperties::Unordered);
        Ok(())
    }

    // --- evaluate_bounds ---

    #[test]
    fn test_ceil_evaluate_bounds_negative() -> Result<()> {
        // ceil([-2.7, -0.2]) = [-2, 0]
        let input = interval_f64(-2.7, -0.2)?;
        let result = SparkCeil::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.lower(), &ScalarValue::Int64(Some(-2)));
        assert_eq!(result.upper(), &ScalarValue::Int64(Some(0)));
        Ok(())
    }

    #[test]
    fn test_floor_evaluate_bounds_negative() -> Result<()> {
        // floor([-2.7, -0.2]) = [-3, -1]
        let input = interval_f64(-2.7, -0.2)?;
        let result = SparkFloor::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.lower(), &ScalarValue::Int64(Some(-3)));
        assert_eq!(result.upper(), &ScalarValue::Int64(Some(-1)));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_f64() -> Result<()> {
        let input = interval_f64(1.5, 49.9)?;
        let result = SparkCeil::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.lower(), &ScalarValue::Int64(Some(2)));
        assert_eq!(result.upper(), &ScalarValue::Int64(Some(50)));
        Ok(())
    }

    #[test]
    fn test_floor_evaluate_bounds_f64() -> Result<()> {
        let input = interval_f64(1.5, 49.9)?;
        let result = SparkFloor::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.lower(), &ScalarValue::Int64(Some(1)));
        assert_eq!(result.upper(), &ScalarValue::Int64(Some(49)));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_exact_int() -> Result<()> {
        // ceil of exact integers is identity
        let input = interval_f64(3.0, 7.0)?;
        let result = SparkCeil::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.lower(), &ScalarValue::Int64(Some(3)));
        assert_eq!(result.upper(), &ScalarValue::Int64(Some(7)));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_decimal128_with_scale() -> Result<()> {
        // ceil(Decimal128(10,2), 0): input [1.50, 49.90] → output [2, 50] as Decimal128(9,0)
        // out type: round_decimal_base(10, 2, 0, true) = (9, 0)
        let input = Interval::try_new(
            ScalarValue::Decimal128(Some(150), 10, 2),
            ScalarValue::Decimal128(Some(4990), 10, 2),
        )?;
        let scale = Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(0)))?;
        let result = SparkCeil::new().evaluate_bounds(&[&input, &scale])?;
        assert_eq!(result.data_type(), DataType::Decimal128(9, 0));
        assert_eq!(result.lower(), &ScalarValue::Decimal128(Some(2), 9, 0));
        assert_eq!(result.upper(), &ScalarValue::Decimal128(Some(50), 9, 0));
        Ok(())
    }

    #[test]
    fn test_floor_evaluate_bounds_decimal128_with_scale() -> Result<()> {
        // floor(Decimal128(10,2), 0): input [1.50, 49.90] → output [1, 49] as Decimal128(9,0)
        let input = Interval::try_new(
            ScalarValue::Decimal128(Some(150), 10, 2),
            ScalarValue::Decimal128(Some(4990), 10, 2),
        )?;
        let scale = Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(0)))?;
        let result = SparkFloor::new().evaluate_bounds(&[&input, &scale])?;
        assert_eq!(result.data_type(), DataType::Decimal128(9, 0));
        assert_eq!(result.lower(), &ScalarValue::Decimal128(Some(1), 9, 0));
        assert_eq!(result.upper(), &ScalarValue::Decimal128(Some(49), 9, 0));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_decimal128_negative_scale() -> Result<()> {
        // ceil(Decimal128(10,2), -2): round up to hundreds
        // input [100.00, 999.99] → output [100, 1000] as Decimal128(9,0)
        // out type: round_decimal_base(10, 2, -2, true) = (9, 0)
        let input = Interval::try_new(
            ScalarValue::Decimal128(Some(10000), 10, 2),
            ScalarValue::Decimal128(Some(99999), 10, 2),
        )?;
        let scale = Interval::try_new(ScalarValue::Int32(Some(-2)), ScalarValue::Int32(Some(-2)))?;
        let result = SparkCeil::new().evaluate_bounds(&[&input, &scale])?;
        assert_eq!(result.data_type(), DataType::Decimal128(9, 0));
        assert_eq!(result.lower(), &ScalarValue::Decimal128(Some(100), 9, 0));
        assert_eq!(result.upper(), &ScalarValue::Decimal128(Some(1000), 9, 0));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_float64_with_scale_returns_unbounded_decimal() -> Result<()> {
        // Float64 input with 2-arg form: can't compute tight bounds (no Decimal128 interval),
        // but must return Decimal128 type (not Int64) to match the function's declared return type.
        let input = interval_f64(1.23, 4.56)?;
        let scale = Interval::try_new(ScalarValue::Int32(Some(2)), ScalarValue::Int32(Some(2)))?;
        let result = SparkCeil::new().evaluate_bounds(&[&input, &scale])?;
        assert!(matches!(result.data_type(), DataType::Decimal128(_, _)));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_2arg_non_tight_scale_falls_back() -> Result<()> {
        // Non-tight scale interval (lower != upper) → unbounded Decimal128
        let input = Interval::try_new(
            ScalarValue::Decimal128(Some(150), 10, 2),
            ScalarValue::Decimal128(Some(4990), 10, 2),
        )?;
        let scale = Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(2)))?;
        let result = SparkCeil::new().evaluate_bounds(&[&input, &scale])?;
        assert_eq!(result.lower(), &ScalarValue::Decimal128(None, 9, 0));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_large_float_out_of_i64_range() -> Result<()> {
        // Float beyond i64::MAX → unbounded fallback (Int64(NULL) bounds), must not saturate
        let input = interval_f64(1e19, 1e20)?;
        let result = SparkCeil::new().evaluate_bounds(&[&input])?;
        assert_eq!(result.data_type(), DataType::Int64);
        assert_eq!(result.lower(), &ScalarValue::Int64(None));
        assert_eq!(result.upper(), &ScalarValue::Int64(None));
        Ok(())
    }

    #[test]
    fn test_ceil_evaluate_bounds_f64_near_i64_max() -> Result<()> {
        // i64::MAX = 2^63-1 is not exactly representable in f64; it rounds up to 2^63.
        // The largest f64 < 2^63 is 9223372036854774784 = i64::MAX - 1023.
        // ceil of that value must NOT saturate to i64::MAX — it should return that value itself.
        let largest_f64_below_i64_max = 9_223_372_036_854_774_784.0_f64;
        assert!(largest_f64_below_i64_max < i64::MAX as f64);
        let result = f64_ceil_to_i64(largest_f64_below_i64_max);
        assert_eq!(result, Some(9_223_372_036_854_774_784_i64));

        // Exactly 2^63 (i64::MAX as f64 rounds up to this) → must return None, not saturate
        let two63 = i64::MAX as f64; // == 2^63
        assert_eq!(f64_ceil_to_i64(two63), None);
        Ok(())
    }

    // --- propagate_constraints ---

    #[test]
    fn test_ceil_propagate_constraints_narrows_input() -> Result<()> {
        // ceil(x) ∈ [5, 10], x originally ∈ [-100.0, 200.0] → [4.0, 10.0]
        let output = interval_i64(5, 10)?;
        let input = interval_f64(-100.0, 200.0)?;
        let result = SparkCeil::new()
            .propagate_constraints(&output, &[&input])?
            .ok_or_else(|| generic_exec_err("test", "expected Some(Vec<Interval>)"))?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].lower(), &ScalarValue::Float64(Some(4.0)));
        assert_eq!(result[0].upper(), &ScalarValue::Float64(Some(10.0)));
        Ok(())
    }

    #[test]
    fn test_floor_propagate_constraints_narrows_input() -> Result<()> {
        // floor(x) ∈ [5, 10], x originally ∈ [-100.0, 200.0] → [5.0, 11.0]
        let output = interval_i64(5, 10)?;
        let input = interval_f64(-100.0, 200.0)?;
        let result = SparkFloor::new()
            .propagate_constraints(&output, &[&input])?
            .ok_or_else(|| generic_exec_err("test", "expected Some(Vec<Interval>)"))?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].lower(), &ScalarValue::Float64(Some(5.0)));
        assert_eq!(result[0].upper(), &ScalarValue::Float64(Some(11.0)));
        Ok(())
    }

    #[test]
    fn test_ceil_propagate_constraints_partial_intersection() -> Result<()> {
        // ceil output [5,10] → constraint [4,10]; input [8,100] → intersection [8,10]
        let output = interval_i64(5, 10)?;
        let input = interval_f64(8.0, 100.0)?;
        let result = SparkCeil::new()
            .propagate_constraints(&output, &[&input])?
            .ok_or_else(|| generic_exec_err("test", "expected Some(Vec<Interval>)"))?;
        assert_eq!(result[0].lower(), &ScalarValue::Float64(Some(8.0)));
        assert_eq!(result[0].upper(), &ScalarValue::Float64(Some(10.0)));
        Ok(())
    }

    #[test]
    fn test_floor_propagate_constraints_partial_intersection() -> Result<()> {
        // floor output [5,10] → constraint [5,11]; input [7,100] → intersection [7,11]
        let output = interval_i64(5, 10)?;
        let input = interval_f64(7.0, 100.0)?;
        let result = SparkFloor::new()
            .propagate_constraints(&output, &[&input])?
            .ok_or_else(|| generic_exec_err("test", "expected Some(Vec<Interval>)"))?;
        assert_eq!(result[0].lower(), &ScalarValue::Float64(Some(7.0)));
        assert_eq!(result[0].upper(), &ScalarValue::Float64(Some(11.0)));
        Ok(())
    }

    #[test]
    fn test_ceil_propagate_constraints_no_overlap() -> Result<()> {
        // constraint [4,10] vs input [20,30] → empty intersection
        let output = interval_i64(5, 10)?;
        let input = interval_f64(20.0, 30.0)?;
        let result = SparkCeil::new().propagate_constraints(&output, &[&input])?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_floor_propagate_constraints_no_overlap() -> Result<()> {
        // constraint [5,11] vs input [20,30] → empty intersection
        let output = interval_i64(5, 10)?;
        let input = interval_f64(20.0, 30.0)?;
        let result = SparkFloor::new().propagate_constraints(&output, &[&input])?;
        assert!(result.is_none());
        Ok(())
    }

    // --- lit_as_integer ---

    #[test]
    fn test_lit_as_integer_various() {
        assert_eq!(lit_as_integer(&ScalarValue::Int32(Some(7))), Some(7));
        assert_eq!(lit_as_integer(&ScalarValue::Float64(Some(5.0))), Some(5));
        assert_eq!(lit_as_integer(&ScalarValue::Float64(Some(5.1))), None);
        assert_eq!(lit_as_integer(&ScalarValue::Float64(Some(f64::NAN))), None);
        assert_eq!(
            lit_as_integer(&ScalarValue::Float64(Some(f64::INFINITY))),
            None
        );
        assert_eq!(
            lit_as_integer(&ScalarValue::Decimal128(Some(500), 10, 2)),
            Some(5)
        );
        assert_eq!(
            lit_as_integer(&ScalarValue::Decimal128(Some(550), 10, 2)),
            None
        );
        assert_eq!(lit_as_integer(&ScalarValue::Float64(None)), None);
    }

    // --- float_bounds ---

    #[test]
    fn test_float_bounds_basic() {
        assert_eq!(float_bounds(5), Some((5.0_f64, 6.0_f64)));
        assert_eq!(float_bounds(0), Some((0.0_f64, 1.0_f64)));
        assert_eq!(float_bounds(-1), Some((-1.0_f64, 0.0_f64)));
    }

    #[test]
    fn test_float_bounds_large_value_returns_none() {
        assert_eq!(float_bounds(i128::MAX), None);
        assert_eq!(float_bounds(i64::MAX as i128 + 1), None);
    }

    // --- extract_tight_scale ---

    #[test]
    fn test_extract_tight_scale_tight() -> Result<()> {
        let interval =
            Interval::try_new(ScalarValue::Int32(Some(-2)), ScalarValue::Int32(Some(-2)))?;
        assert_eq!(extract_tight_scale(&interval), Some(-2));
        Ok(())
    }

    #[test]
    fn test_extract_tight_scale_loose_returns_none() -> Result<()> {
        let interval = Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(2)))?;
        assert_eq!(extract_tight_scale(&interval), None);
        Ok(())
    }

    // --- decimal128_bounds ---

    #[test]
    fn test_decimal128_bounds_basic() {
        // floor(x) = 5 with Decimal128(10,2): x ∈ [5.00, 6.00) → stored as [500, 600]
        assert_eq!(
            decimal128_bounds(5, 10, 2),
            Some((
                ScalarValue::Decimal128(Some(500), 10, 2),
                ScalarValue::Decimal128(Some(600), 10, 2),
            ))
        );
    }

    #[test]
    fn test_decimal128_bounds_negative_scale_returns_none() {
        assert_eq!(decimal128_bounds(5, 10, -1), None);
    }

    // --- i64_to_input_scalar ---

    #[test]
    fn test_i64_to_input_scalar_fits() {
        assert_eq!(
            i64_to_input_scalar(5, &DataType::Float64),
            Some(ScalarValue::Float64(Some(5.0)))
        );
        assert_eq!(
            i64_to_input_scalar(5, &DataType::Int32),
            Some(ScalarValue::Int32(Some(5)))
        );
        assert_eq!(
            i64_to_input_scalar(5, &DataType::Int64),
            Some(ScalarValue::Int64(Some(5)))
        );
    }

    #[test]
    fn test_i64_to_input_scalar_overflow_returns_none() {
        assert_eq!(i64_to_input_scalar(300, &DataType::Int8), None);
    }

    // --- preimage_floor direct tests ---

    #[test]
    fn test_preimage_floor_float64_integer_rhs() -> Result<()> {
        // floor(a) = 5 with a: Float64 → x ∈ [5.0, 6.0)
        let col = Expr::Column(datafusion_common::Column::from_name("a"));
        let lit = Expr::Literal(ScalarValue::Int64(Some(5)), None);
        let ctx = make_simplify_ctx("a", DataType::Float64)?;
        let result = preimage_floor(&[col], &lit, &ctx)?;
        if let PreimageResult::Range { interval, .. } = result {
            assert_eq!(interval.lower(), &ScalarValue::Float64(Some(5.0)));
            assert_eq!(interval.upper(), &ScalarValue::Float64(Some(6.0)));
        } else {
            return Err(generic_exec_err("test", "expected PreimageResult::Range"));
        }
        Ok(())
    }

    #[test]
    fn test_preimage_floor_float64_fractional_rhs_returns_none() -> Result<()> {
        // floor always returns integer, so fractional RHS → PreimageResult::None
        let col = Expr::Column(datafusion_common::Column::from_name("a"));
        let lit = Expr::Literal(ScalarValue::Float64(Some(5.5)), None);
        let ctx = make_simplify_ctx("a", DataType::Float64)?;
        let result = preimage_floor(&[col], &lit, &ctx)?;
        assert!(matches!(result, PreimageResult::None));
        Ok(())
    }

    #[test]
    fn test_preimage_floor_decimal128() -> Result<()> {
        // floor(a) = 5 with a: Decimal128(10,2) → x ∈ [5.00, 6.00) stored as [500, 600]
        let col = Expr::Column(datafusion_common::Column::from_name("a"));
        let lit = Expr::Literal(ScalarValue::Int64(Some(5)), None);
        let ctx = make_simplify_ctx("a", DataType::Decimal128(10, 2))?;
        let result = preimage_floor(&[col], &lit, &ctx)?;
        if let PreimageResult::Range { interval, .. } = result {
            assert_eq!(interval.lower(), &ScalarValue::Decimal128(Some(500), 10, 2));
            assert_eq!(interval.upper(), &ScalarValue::Decimal128(Some(600), 10, 2));
        } else {
            return Err(generic_exec_err("test", "expected PreimageResult::Range"));
        }
        Ok(())
    }

    #[test]
    fn test_preimage_floor_multiarg_returns_none() -> Result<()> {
        // 2-arg form: preimage not supported
        let col = Expr::Column(datafusion_common::Column::from_name("a"));
        let scale = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        let lit = Expr::Literal(ScalarValue::Int64(Some(5)), None);
        let ctx = make_simplify_ctx("a", DataType::Float64)?;
        let result = preimage_floor(&[col, scale], &lit, &ctx)?;
        assert!(matches!(result, PreimageResult::None));
        Ok(())
    }
}
