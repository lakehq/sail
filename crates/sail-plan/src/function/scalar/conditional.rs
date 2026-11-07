use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast, when};
use sail_common_datafusion::type_coercion::binary_type_coercion::string_promotion_type;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::conditional::coalesce::CoalesceFunc;
use sail_function::scalar::conditional::nullif::NullIfFunc;
use sail_function::scalar::conversion::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::conversion::spark_to_string::{
    SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View,
};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

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
    let branch_values = coerce_date_timestamp_values(branch_values, &function_context)?;
    let branch_values = coerce_string_temporal_values(branch_values, &function_context)?;
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
    let values = coerce_date_timestamp_values(vec![then_expr, else_expr], &function_context)?;
    let (then_expr, else_expr) = coerce_string_temporal_values(values, &function_context)?.two()?;
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
    // Like Spark's `NullPropagation`, drop the NULL literals, which never supply a value. After
    // type coercion, such an argument would be a cast without the interval range of the other
    // arguments. A typed NULL literal is kept since its type takes part in the result type.
    let is_null = |x: &expr::Expr| matches!(x, expr::Expr::Literal(ScalarValue::Null, _));
    let arguments = if arguments.iter().all(is_null) {
        arguments
    } else {
        arguments.into_iter().filter(|x| !is_null(x)).collect()
    };
    let arguments = coerce_date_timestamp_values(arguments, &function_context)?;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    let arguments = coerce_string_promotion_values(arguments, &function_context)?;
    Ok(ScalarUDF::from(CoalesceFunc::new()).call(arguments))
}

/// Casts STRING values mixed with numeric, boolean, or binary values to their common type,
/// following Spark's string promotion (see `string_promotion_type`). Other values are left to the
/// type coercion of the function.
fn coerce_string_promotion_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let schema = function_context.schema;
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(schema))
        .collect::<Result<Vec<_>, _>>()?;
    // Values of other types keep the type coercion of the function, and temporal values with
    // STRING values are coerced by `coerce_string_temporal_values` already.
    let is_string = |t: &DataType| match t {
        DataType::Dictionary(_, value) => value.is_string(),
        t => t.is_string(),
    };
    if !data_types.iter().any(is_string)
        || !data_types.iter().any(|t| !t.is_null() && !is_string(t))
    {
        return Ok(arguments);
    }
    let Some(common_type) =
        string_promotion_type(&data_types, function_context.plan_config.ansi_mode)
    else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .zip(data_types)
        .map(|(arg, data_type)| -> PlanResult<expr::Expr> {
            if data_type == common_type || data_type.is_null() {
                return Ok(arg);
            }
            // Like Spark's cast, numeric values are formatted as Spark does.
            Ok(match &common_type {
                DataType::Utf8 => ScalarUDF::from(SparkToUtf8::new()).call(vec![arg]),
                DataType::LargeUtf8 => ScalarUDF::from(SparkToLargeUtf8::new()).call(vec![arg]),
                DataType::Utf8View => ScalarUDF::from(SparkToUtf8View::new()).call(vec![arg]),
                _ => arg.cast_to(&common_type, schema)?,
            })
        })
        .collect()
}

/// Spark defines `nvl2` as `if` on whether the first argument is not null.
fn nvl2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, then_value, else_value) = arguments.three()?;
    if_expr(ScalarFunctionInput {
        arguments: vec![value.is_not_null(), then_value, else_value],
        function_context,
    })
}

/// Spark's `nullif` returns its first argument's type, comparing the arguments in their common type.
/// Like Spark's `=`, the comparison treats `-0.0` and `0.0` as equal.
fn nullif(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (left, right) = arguments.two()?;
    let schema = function_context.schema;
    let left_type = left.get_type(schema)?;
    // The first argument is evaluated twice, so a volatile one keeps the native function.
    // TODO: The native function returns the common type rather than the first argument's type.
    if left.is_volatile() {
        return Ok(ScalarUDF::from(NullIfFunc::new()).call(vec![left, right]));
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
    // The native function returns the common type of the arguments, which is the type of the
    // first argument here. Unlike a CASE expression, it is always nullable, as in Spark.
    let right_type = right.get_type(schema)?;
    if right_type == left_type || right_type.is_null() {
        return Ok(ScalarUDF::from(NullIfFunc::new()).call(vec![left, right]));
    }
    // Like Spark's comparison, a DATE compares to a TIMESTAMP at the start of its day in the
    // session time zone, and a TIMESTAMP_NTZ compares to a TIMESTAMP in the session time zone.
    // Other values compare as they are, without a time zone.
    let (left_value, right_value) =
        coerce_date_timestamp_values(vec![left.clone(), right], &function_context)?.two()?;
    let left_value_type = left_value.get_type(schema)?;
    let right_value_type = right_value.get_type(schema)?;
    let condition = if is_timestamp_ntz_mix(&[left_value_type.clone(), right_value_type.clone()]) {
        cast_to_session_timestamp(left_value, &left_value_type, timezone).eq(
            cast_to_session_timestamp(right_value, &right_value_type, timezone),
        )
    } else {
        left_value.eq(right_value)
    };
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

/// Casts DATE values to TIMESTAMP if other values are timestamps without string values.
///
/// Spark casts a DATE to TIMESTAMP at the start of its day in the session time zone, whatever
/// zone labels the other values, but the common type of DATE and TIMESTAMP in type coercion
/// does not keep the time zone. The other values are cast to TIMESTAMP in the session time zone
/// as well, so that the common type is TIMESTAMP in the session time zone.
// TODO: Cast DATE values nested in arrays, maps, and structs as well.
fn coerce_date_timestamp_values(
    values: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = values
        .iter()
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if data_types.iter().any(is_string_type) || !data_types.iter().any(is_date_type) {
        return Ok(values);
    }
    let local = if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        true
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        false
    } else {
        return Ok(values);
    };
    let timezone = &function_context.plan_config.session_timezone_label();
    Ok(values
        .into_iter()
        .zip(data_types)
        .map(|(value, data_type)| {
            let (value, data_type) = if is_date_type(&data_type) {
                let data_type = DataType::Timestamp(TimeUnit::Microsecond, None);
                (cast(value, data_type.clone()), data_type)
            } else {
                (value, data_type)
            };
            if local {
                cast_to_session_timestamp(value, &data_type, timezone)
            } else {
                value
            }
        })
        .collect())
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
    Ok(ScalarUDF::from(CoalesceFunc::new()).call(vec![arg, zero]))
}
