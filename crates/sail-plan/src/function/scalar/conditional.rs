use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast, when};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::timestamp_cast::cast_timestamp_ntz_to_ltz;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::function::datetime_utils::{
    cast_to_session_timestamp, coerce_timestamp_ntz_values, is_timestamp_ntz_mix,
};

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                conditions.push(lit(true));
                branch_values.push(condition);
                break;
            }
        }
    }
    let branch_values = coerce_string_temporal_values(branch_values, &function_context)?;
    let branch_values = coerce_decimal_to_spark_wider_type(branch_values, function_context.schema)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_string_temporal_values(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    let arguments = coerce_decimal_to_spark_wider_type(arguments, function_context.schema)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Casts numeric arguments that mix decimals with other numeric types to Spark's wider type.
/// The wider type of a decimal and a floating-point type is `double`. The wider type of
/// decimals and integers is the decimal with the most integral digits and the largest scale,
/// where an integer has as many digits as its type can hold.
pub(super) fn coerce_decimal_to_spark_wider_type(
    arguments: Vec<expr::Expr>,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(schema))
        .collect::<Result<Vec<_>, _>>()?;
    if !data_types
        .iter()
        .any(|t| matches!(t, DataType::Decimal128(..)))
    {
        return Ok(arguments);
    }
    let target = if data_types.iter().any(DataType::is_floating) {
        DataType::Float64
    } else {
        let mut integral_digits = 0;
        let mut scale = 0;
        for data_type in &data_types {
            let (p, s) = match data_type {
                DataType::Decimal128(p, s) => (i16::from(*p), i16::from(*s)),
                DataType::Int8 => (3, 0),
                DataType::Int16 => (5, 0),
                DataType::Int32 => (10, 0),
                DataType::Int64 => (20, 0),
                _ => continue,
            };
            integral_digits = integral_digits.max(p - s);
            scale = scale.max(s);
        }
        let precision = (integral_digits + scale).min(38);
        let scale = scale.min(precision);
        DataType::Decimal128(precision as u8, scale as i8)
    };
    Ok(arguments
        .into_iter()
        .zip(data_types)
        .map(|(arg, data_type)| {
            if data_type.is_numeric() && data_type != target {
                cast(arg, target.clone())
            } else {
                arg
            }
        })
        .collect())
}

/// Spark defines `nvl2` as `if` on whether the first argument is not null.
fn nvl2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, then_value, else_value) = arguments.three()?;
    let schema = function_context.schema;
    let then_type = then_value.get_type(schema)?;
    let else_type = else_value.get_type(schema)?;
    let is_timestamp = |data_type: &DataType| matches!(data_type, DataType::Timestamp(_, Some(_)));
    let date_timestamp_mix = (is_date_type(&then_type) && is_timestamp(&else_type))
        || (is_timestamp(&then_type) && is_date_type(&else_type));
    // TODO: Use `if` for all types once it returns the common type of its branches.
    if !date_timestamp_mix && !is_timestamp_ntz_mix(&[then_type.clone(), else_type.clone()]) {
        return Ok(expr_fn::nvl2(value, then_value, else_value));
    }
    // Spark casts a DATE to TIMESTAMP at the start of its day in the session time zone, whatever
    // zone labels the other value.
    let timezone = &function_context.plan_config.session_timezone_label();
    // The cast of midnight converts like Spark in a DST gap or overlap, where Arrow's cast fails.
    let session_date = |value: expr::Expr, data_type: &DataType| {
        if is_date_type(data_type) {
            cast_timestamp_ntz_to_ltz(
                cast(value, DataType::Timestamp(TimeUnit::Microsecond, None)),
                timezone,
                false,
            )
        } else {
            value
        }
    };
    let then_value = session_date(then_value, &then_type);
    let else_value = session_date(else_value, &else_type);
    if_expr(ScalarFunctionInput {
        arguments: vec![value.is_not_null(), then_value, else_value],
        function_context,
    })
}

/// Spark's `nullif` returns its first argument's type, comparing the arguments in their common type.
fn nullif(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (left, right) = arguments.two()?;
    let schema = function_context.schema;
    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;
    // The first argument is evaluated twice, so a volatile one keeps the native function.
    // TODO: The native function returns the common type rather than the first argument's type.
    if left.is_volatile() || !is_timestamp_ntz_mix(&[left_type.clone(), right_type.clone()]) {
        return Ok(expr_fn::nullif(left, right));
    }
    let timezone = &function_context.plan_config.session_timezone_label();
    // TODO: `date_trunc` returns TIMESTAMP_NTZ for a TIMESTAMP_NTZ value, where Spark returns
    //  TIMESTAMP. Remove this once it does. A value in other units than microseconds is still
    //  cast after `date_trunc`, which this misses.
    let (left, left_type) = match &left {
        expr::Expr::ScalarFunction(function)
            if function.name() == "date_trunc"
                && matches!(left_type, DataType::Timestamp(_, None)) =>
        {
            let left = cast_to_session_timestamp(left, &left_type, timezone);
            let left_type = left.get_type(schema)?;
            (left, left_type)
        }
        _ => (left, left_type),
    };
    let condition = cast_to_session_timestamp(left.clone(), &left_type, timezone)
        .eq(cast_to_session_timestamp(right, &right_type, timezone));
    Ok(when(condition, lit(ScalarValue::try_from(&left_type)?)).otherwise(left)?)
}

/// Spark defines `nvl` (and its alias `ifnull`) as `coalesce` with two arguments.
fn nvl(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (left, right) = arguments.two()?;
    coalesce(ScalarFunctionInput {
        arguments: vec![left, right],
        function_context,
    })
}

fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else {
        coerce_timestamp_ntz_values(
            arguments,
            function_context.schema,
            &function_context.plan_config.session_timezone_label(),
        )?
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn common_temporal_type(data_types: &[DataType], session_timezone: &Arc<str>) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::custom(nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::custom(nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(nvl)),
        ("nvl2", F::custom(nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let condition = match arg.get_type(function_context.schema)? {
        data_type if is_string_type(&data_type) => {
            let value = if ansi_mode {
                cast(arg.clone(), DataType::Int64)
            } else {
                ScalarUDF::from(SparkCastStringToInt32::new()).call(vec![arg.clone()])
            };
            value.eq(lit(0_i32))
        }
        DataType::Boolean if !ansi_mode => arg.clone().eq(lit(false)),
        data_type if data_type.is_numeric() || data_type == DataType::Null => {
            arg.clone().eq(lit(0_i32))
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "nullifzero cannot compare data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(when(condition, lit(ScalarValue::Null)).otherwise(arg)?)
}

fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let (arg, zero) = match arg.get_type(function_context.schema)? {
        DataType::Float32 if ansi_mode => (cast(arg, DataType::Float64), lit(0_i32)),
        data_type if is_string_type(&data_type) && ansi_mode => {
            (cast(arg, DataType::Int64), lit(0_i32))
        }
        data_type if is_string_type(&data_type) => (arg, lit("0")),
        DataType::Decimal128(precision, scale) => {
            // The INT zero contributes ten integer digits. Keep integer digits when
            // the common decimal type exceeds Spark's 38-digit precision limit.
            let integer_digits = (i16::from(precision) - i16::from(scale)).max(10);
            let scale = i16::from(scale).max(0);
            let precision = (integer_digits + scale).min(38) as u8;
            let scale = scale.min((38 - integer_digits).max(0)) as i8;
            let data_type = DataType::Decimal128(precision, scale);
            let arg = if ansi_mode {
                cast(arg, data_type)
            } else {
                try_cast(arg, data_type)
            };
            (arg, lit(0_i32))
        }
        data_type if data_type.is_numeric() || data_type == DataType::Null => (arg, lit(0_i32)),
        data_type => {
            return Err(PlanError::invalid(format!(
                "zeroifnull cannot combine data type {data_type} with an integer zero"
            )));
        }
    };
    Ok(expr_fn::coalesce(vec![arg, zero]))
}
