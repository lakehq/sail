use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_div::SparkDiv;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_sqrt::SparkSqrt;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_add(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok(match values.len() {
        1 => values.one()?,
        _ => {
            let (left, right) = values.two()?;
            arithmetic(context, name, left, right)?
        }
    })
}

fn resolve_subtract(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok(match values.len() {
        1 => negate(context, values.one()?)?,
        _ => {
            let (left, right) = values.two()?;
            arithmetic(context, name, left, right)?
        }
    })
}

fn resolve_multiply(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (left, right) = values.two()?;
        arithmetic(context, name, left, right)?
    })
}

fn resolve_negative(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    negate(&mut input.context, input.arguments.one()?)
}

fn resolve_positive(input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    Ok(input.arguments.one()?)
}

fn resolve_acos(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let expected = if matches!(name, "atan2" | "log") {
            2
        } else {
            1
        };
        crate::analyzer::typing::coercion::validate_arity(name, values.len(), expected)?;
        let mut values = values
            .into_iter()
            .map(|value| context.cast(value, DataType::Float64))
            .collect::<PlanResult<Vec<_>>>()?;
        match name {
            "sqrt" => context.scalar_udf(Arc::new(ScalarUDF::from(SparkSqrt::new())), values)?,
            "csc" | "sec" => {
                let denominator =
                    context.scalar(if name == "csc" { "sin" } else { "cos" }, values)?;
                let one = context.literal(ScalarValue::Float64(Some(1.0)));
                context.binary(one, Operator::Divide, denominator)?
            }
            "ln" | "log" | "log10" | "log1p" | "log2" => {
                let mut value = values
                    .pop()
                    .ok_or_else(|| PlanError::internal("missing logarithm argument"))?;
                if name == "log1p" {
                    let one = context.literal(ScalarValue::Float64(Some(1.0)));
                    value = context.binary(value, Operator::Plus, one)?;
                }
                let copy = context.copy_expression(&value)?;
                let zero = context.literal(ScalarValue::Float64(Some(0.0)));
                let positive = context.binary(copy, Operator::Gt, zero)?;
                values.push(context.case(vec![(positive, value)], None)?);
                context.scalar(if name == "log1p" { "ln" } else { name }, values)?
            }
            _ => context.scalar(name, values)?,
        }
    })
}

fn resolve_ceil(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let ansi = context.config.ansi_mode;
    Ok({
        let mut values = values;
        if values.len() == 1
            && matches!(
                values[0].result_type.resolved("rounding input")?.data_type,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            let value = values.one()?;
            return context.cast(value, DataType::Int64);
        }
        if values.len() == 2 {
            let scale = values
                .pop()
                .ok_or_else(|| PlanError::internal("missing scale"))?;
            let value = crate::analyzer::lowering::evaluate_constant(&scale)?;
            if value.data_type() != DataType::Int32 {
                return Err(PlanError::invalid("Target scale must be Integer literal"));
            }
            values.push(context.literal(value.cast_to(&DataType::Int32)?));
        }
        let function = if name == "floor" {
            ScalarUDF::from(SparkFloor::new(ansi))
        } else {
            ScalarUDF::from(SparkCeil::new(ansi))
        };
        context.scalar_udf(Arc::new(function), values)?
    })
}

fn resolve_bin(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let ansi = context.config.ansi_mode;
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkBin::new(ansi))), values)
}

fn resolve_pmod(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let ansi = context.config.ansi_mode;
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkPmod::new(ansi))), values)
}

fn resolve_pow(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let result = context.scalar("power", values)?;
        context.cast(result, DataType::Float64)?
    })
}

fn resolve_hypot(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (a, b) = values.two()?;
        let a_copy = context.copy_expression(&a)?;
        let b_copy = context.copy_expression(&b)?;
        let a = context.binary(a, Operator::Multiply, a_copy)?;
        let b = context.binary(b, Operator::Multiply, b_copy)?;
        let value = context.binary(a, Operator::Plus, b)?;
        let result = context.scalar("sqrt", vec![value])?;
        context.cast(result, DataType::Float64)?
    })
}

fn resolve_rint(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let result = context.scalar("round", vec![values.one()?])?;
        context.cast(result, DataType::Float64)?
    })
}

fn resolve_round(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let mut values = values;
        if let Some(first) = values.first()
            && first
                .result_type
                .resolved("round value")?
                .data_type
                .is_string()
        {
            let scale = match values.get(1) {
                Some(scale) => context.copy_expression(scale)?,
                None => context.literal(ScalarValue::Int32(Some(0))),
            };
            let condition = context.is_null(scale);
            let null = context.literal(ScalarValue::Utf8(None));
            let value = values.remove(0);
            let value = context.case(vec![(condition, null)], Some(value))?;
            let value = string_to_double(context, value)?;
            let value = context.try_cast(value, DataType::Float64)?;
            values.insert(0, value);
        }
        context.scalar("round", values)?
    })
}

fn resolve_e(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        context.literal(ScalarValue::Float64(Some(std::f64::consts::E)))
    })
}

pub(crate) fn string_to_double(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    if context.config.ansi_mode {
        context.cast(value, DataType::Float64)
    } else {
        context.try_cast(value, DataType::Float64)
    }
}

pub(crate) fn negate(context: &mut FunctionContextInput<'_>, value: Expr) -> PlanResult<Expr> {
    let data_type = value
        .result_type
        .resolved("negation input")?
        .data_type
        .clone();
    if matches!(data_type, DataType::Duration(_)) {
        return context.scalar_udf(
            Arc::new(ScalarUDF::from(NegateDuration::new())),
            vec![value],
        );
    }
    if data_type.is_string() {
        let value = string_to_double(context, value)?;
        return context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkNegative::new(
                context.config.ansi_mode,
            ))),
            vec![value],
        );
    }
    if data_type.is_floating() {
        return Ok(Expr {
            id: context.ids.expression(),
            result_type: value.result_type.clone(),
            kind: Deferred::Resolved(ExprKind::Negative(Box::new(value))),
        });
    }
    let folded = if let Deferred::Resolved(ExprKind::Literal(value)) = &value.kind {
        match value {
            ScalarValue::Int8(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int8(Some(v))),
            ScalarValue::Int16(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int16(Some(v))),
            ScalarValue::Int32(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int32(Some(v))),
            ScalarValue::Int64(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int64(Some(v))),
            _ => None,
        }
    } else {
        None
    };
    if let Some(value) = folded {
        return Ok(context.literal(value));
    }
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkNegative::new(
            context.config.ansi_mode,
        ))),
        vec![value],
    )
}

pub(crate) fn arithmetic(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    mut left: Expr,
    mut right: Expr,
) -> PlanResult<Expr> {
    let left_type = left
        .result_type
        .resolved("arithmetic left")?
        .data_type
        .clone();
    let right_type = right
        .result_type
        .resolved("arithmetic right")?
        .data_type
        .clone();
    let duration = DataType::Duration(TimeUnit::Microsecond);
    let operator = match name {
        "+" => Operator::Plus,
        "-" => Operator::Minus,
        "*" => Operator::Multiply,
        "/" | "div" => Operator::Divide,
        _ => Operator::Modulo,
    };
    if name == "+"
        && ((left_type.is_string() && right_type == duration)
            || (right_type.is_string() && left_type == duration))
    {
        let string_type = if left_type.is_string() {
            left_type
        } else {
            std::mem::swap(&mut left, &mut right);
            right_type
        };
        let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
            Some(context.config.session_timezone.clone()),
            context.config.ansi_mode,
            false,
        )?);
        let left = context.scalar_udf(Arc::new(timestamp), vec![left])?;
        let right = context.scalar_udf(
            Arc::new(ScalarUDF::from(
                SparkDayTimeIntervalToCalendarInterval::new(),
            )),
            vec![right],
        )?;
        let value = context.binary(left, operator, right)?;
        let function = match string_type {
            DataType::LargeUtf8 => ScalarUDF::from(SparkToLargeUtf8::new()),
            DataType::Utf8View => ScalarUDF::from(SparkToUtf8View::new()),
            _ => ScalarUDF::from(SparkToUtf8::new()),
        };
        return context.scalar_udf(Arc::new(function), vec![value]);
    }
    if matches!(name, "+" | "-") {
        if left_type == DataType::Date32 && right_type == duration {
            right = context.cast(right, DataType::Interval(IntervalUnit::MonthDayNano))?;
        } else if name == "+" && right_type == DataType::Date32 && left_type == duration {
            left = context.cast(left, DataType::Interval(IntervalUnit::MonthDayNano))?;
        } else if left_type == DataType::Date32 && right_type.is_numeric() {
            let left = context.cast(left, DataType::Int32)?;
            let value = context.binary(left, operator, right)?;
            return context.cast(value, DataType::Date32);
        } else if name == "+" && right_type == DataType::Date32 && left_type.is_numeric() {
            let right = context.cast(right, DataType::Int32)?;
            let value = context.binary(left, operator, right)?;
            return context.cast(value, DataType::Date32);
        }
    }
    if matches!(name, "/" | "div" | "%" | "mod") {
        if is_zero_literal(&right) {
            if !context.config.ansi_mode {
                return Ok(context.literal(ScalarValue::Null));
            }
            return Err(PlanError::ArrowError(if matches!(name, "%" | "mod") {
                arrow::error::ArrowError::ArithmeticOverflow("Remainder by zero".into())
            } else {
                arrow::error::ArrowError::DivideByZero
            }));
        }
        right = safe_divisor(
            context,
            right,
            &right_type,
            if matches!(name, "%" | "mod") {
                "Remainder by zero"
            } else {
                "Division by zero"
            },
        )?;
    }
    let value = if name == "div" {
        if matches!(
            (&left_type, &right_type),
            (DataType::Duration(_), DataType::Duration(_))
        ) {
            let left = context.cast(left, DataType::Int64)?;
            let right = context.cast(right, DataType::Int64)?;
            context.binary(left, operator, right)?
        } else {
            context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkDiv::new())),
                vec![left, right],
            )?
        }
    } else if matches!(name, "*" | "/")
        && (left_type == duration || (name == "*" && right_type == duration))
    {
        if left_type == duration {
            left = context.cast(left, DataType::Int64)?;
        }
        if right_type == duration {
            right = context.cast(right, DataType::Int64)?;
        }
        let value = context.binary(left, operator, right)?;
        return context.cast(value, duration);
    } else if name == "/"
        && !matches!(
            left_type,
            DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime)
        )
        && !matches!(
            right_type,
            DataType::Decimal128(..) | DataType::Decimal256(..)
        )
    {
        let left = context.cast(left, DataType::Float64)?;
        let right = context.cast(right, DataType::Float64)?;
        context.binary(left, operator, right)?
    } else {
        context.binary(left, operator, right)?
    };
    if name == "div" {
        context.cast(value, DataType::Int64)
    } else {
        Ok(value)
    }
}

pub(crate) fn safe_divisor(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    data_type: &DataType,
    message: &str,
) -> PlanResult<Expr> {
    if matches!(data_type, DataType::Interval(_) | DataType::Duration(_)) {
        return Ok(value);
    }
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let copy = context.copy_expression(&value)?;
    let mut condition = context.binary(copy, Operator::Eq, zero)?;
    if matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        let negative_zero = context.literal(ScalarValue::Float64(Some(-0.0)).cast_to(data_type)?);
        let copy = context.copy_expression(&value)?;
        let negative = context.binary(copy, Operator::Eq, negative_zero)?;
        condition = context.binary(condition, Operator::Or, negative)?;
    }
    let invalid = if context.config.ansi_mode {
        let message = context.literal(ScalarValue::Utf8(Some(message.into())));
        context.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])?
    } else {
        context.literal(ScalarValue::Null)
    };
    context.case(vec![(condition, invalid)], Some(value))
}

fn is_zero_literal(expression: &Expr) -> bool {
    match &expression.kind {
        Deferred::Resolved(ExprKind::Literal(value)) => {
            value.data_type().is_numeric()
                && !value.is_null()
                && value
                    .cast_to(&DataType::Float64)
                    .is_ok_and(|v| matches!(v, ScalarValue::Float64(Some(value)) if value == 0.0))
        }
        Deferred::Resolved(
            ExprKind::Cast { expression, .. } | ExprKind::WithMetadata(expression),
        ) => is_zero_literal(expression),
        _ => false,
    }
}

pub(super) fn list_built_in_math_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "abs",
            F::custom(|mut input| {
                let config = input.context.config;
                let kernel = Arc::new(ScalarUDF::from(
                    sail_function::scalar::math::spark_abs::SparkAbs::new(config.ansi_mode),
                ));
                input.context.scalar_udf(kernel, input.arguments)
            }),
        ),
        (
            "bround",
            F::udf(sail_function::scalar::math::spark_bround::SparkBRound::new()),
        ),
        (
            "conv",
            F::udf(sail_function::scalar::math::spark_conv::SparkConv::new()),
        ),
        (
            "expm1",
            F::scalar_udf(datafusion_spark::function::math::expm1),
        ),
        ("factorial", F::kernel("factorial")),
        ("greatest", F::kernel("greatest")),
        ("hex", F::scalar_udf(datafusion_spark::function::math::hex)),
        ("least", F::kernel("least")),
        ("pi", F::kernel("pi")),
        (
            "rand",
            F::udf(sail_function::scalar::math::random::Random::new()),
        ),
        (
            "randn",
            F::udf(sail_function::scalar::math::randn::Randn::new()),
        ),
        (
            "random",
            F::udf(sail_function::scalar::math::random::Random::new()),
        ),
        (
            "random_poisson",
            F::udf(sail_function::scalar::math::rand_poisson::RandPoisson::new()),
        ),
        (
            "sign",
            F::udf(sail_function::scalar::math::spark_signum::SparkSignum::new()),
        ),
        (
            "signum",
            F::udf(sail_function::scalar::math::spark_signum::SparkSignum::new()),
        ),
        (
            "try_add",
            F::udf(sail_function::scalar::math::spark_try_add::SparkTryAdd::new()),
        ),
        (
            "try_divide",
            F::udf(sail_function::scalar::math::spark_try_div::SparkTryDiv::new()),
        ),
        (
            "try_mod",
            F::udf(sail_function::scalar::math::spark_try_mod::SparkTryMod::new()),
        ),
        (
            "try_multiply",
            F::udf(sail_function::scalar::math::spark_try_mult::SparkTryMult::new()),
        ),
        (
            "try_subtract",
            F::udf(sail_function::scalar::math::spark_try_subtract::SparkTrySubtract::new()),
        ),
        (
            "unhex",
            F::udf(sail_function::scalar::math::spark_unhex::SparkUnHex::new()),
        ),
        (
            "uniform",
            F::udf(sail_function::scalar::math::spark_uniform::SparkUniform::new()),
        ),
        (
            "width_bucket",
            F::scalar_udf(datafusion_spark::function::math::width_bucket),
        ),
        ("+", F::custom(|input| resolve_add(input, "+"))),
        ("-", F::custom(|input| resolve_subtract(input, "-"))),
        ("*", F::custom(|input| resolve_multiply(input, "*"))),
        ("/", F::custom(|input| resolve_multiply(input, "/"))),
        ("%", F::custom(|input| resolve_multiply(input, "%"))),
        ("mod", F::custom(|input| resolve_multiply(input, "mod"))),
        ("div", F::custom(|input| resolve_multiply(input, "div"))),
        ("negative", F::custom(resolve_negative)),
        ("positive", F::custom(resolve_positive)),
        ("acos", F::custom(|input| resolve_acos(input, "acos"))),
        ("acosh", F::custom(|input| resolve_acos(input, "acosh"))),
        ("asin", F::custom(|input| resolve_acos(input, "asin"))),
        ("asinh", F::custom(|input| resolve_acos(input, "asinh"))),
        ("atan", F::custom(|input| resolve_acos(input, "atan"))),
        ("atan2", F::custom(|input| resolve_acos(input, "atan2"))),
        ("atanh", F::custom(|input| resolve_acos(input, "atanh"))),
        ("cbrt", F::custom(|input| resolve_acos(input, "cbrt"))),
        ("cos", F::custom(|input| resolve_acos(input, "cos"))),
        ("cosh", F::custom(|input| resolve_acos(input, "cosh"))),
        ("cot", F::custom(|input| resolve_acos(input, "cot"))),
        ("degrees", F::custom(|input| resolve_acos(input, "degrees"))),
        ("exp", F::custom(|input| resolve_acos(input, "exp"))),
        ("radians", F::custom(|input| resolve_acos(input, "radians"))),
        ("sin", F::custom(|input| resolve_acos(input, "sin"))),
        ("sinh", F::custom(|input| resolve_acos(input, "sinh"))),
        ("tan", F::custom(|input| resolve_acos(input, "tan"))),
        ("tanh", F::custom(|input| resolve_acos(input, "tanh"))),
        ("ln", F::custom(|input| resolve_acos(input, "ln"))),
        ("log", F::custom(|input| resolve_acos(input, "log"))),
        ("log10", F::custom(|input| resolve_acos(input, "log10"))),
        ("log1p", F::custom(|input| resolve_acos(input, "log1p"))),
        ("log2", F::custom(|input| resolve_acos(input, "log2"))),
        ("sqrt", F::custom(|input| resolve_acos(input, "sqrt"))),
        ("csc", F::custom(|input| resolve_acos(input, "csc"))),
        ("sec", F::custom(|input| resolve_acos(input, "sec"))),
        ("ceil", F::custom(|input| resolve_ceil(input, "ceil"))),
        ("ceiling", F::custom(|input| resolve_ceil(input, "ceiling"))),
        ("floor", F::custom(|input| resolve_ceil(input, "floor"))),
        ("bin", F::custom(resolve_bin)),
        ("pmod", F::custom(resolve_pmod)),
        ("pow", F::custom(resolve_pow)),
        ("power", F::custom(resolve_pow)),
        ("hypot", F::custom(resolve_hypot)),
        ("rint", F::custom(resolve_rint)),
        ("round", F::custom(resolve_round)),
        ("e", F::custom(resolve_e)),
    ]
}
