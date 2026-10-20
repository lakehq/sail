use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{
    BinaryExpr, ExprSchemable, HigherOrderUDF, Operator, ScalarUDF, ScalarUDFImpl, Volatility,
    cast, lit, try_cast, when,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use sail_common::utils::datetime::{MICROS_PER_DAY, time_unit_to_multiplier};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::session::plan::PlanFormatter;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy, SparkToLocalTime};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::{SparkDateTrunc, SparkStartOfDay};
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_dt_interval::SparkMakeDtInterval;
use sail_function::scalar::datetime::spark_make_interval::SparkMakeInterval;
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_timestamp_add::SparkTimestampAdd;
use sail_function::scalar::datetime::spark_timestamp_from_number::{
    SparkTimestampFromNumber, TimestampNumberMode,
};
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_python_udf::udf::expr_contains_python_udf;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use super::lambda::lambda_with_fresh_parameter;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, expr_contains_spark_cast_to_variant,
};
use crate::function::datetime_utils::{
    cast_temporal, cast_timestamp_ntz_to_ltz, timestamp_epoch_units,
};

fn wrong_datetime_arity(name: &str, expected: &str, actual: usize) -> PlanError {
    PlanError::invalid(format!(
        "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] \
         The `{name}` requires {expected} parameters \
         but the actual number is {actual}."
    ))
}

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        ScalarUDF::from(SparkDatePart::new()).call(vec![lit(part.to_uppercase()), expr]),
        DataType::Int32,
    )
}

pub(super) fn datetime_date(expr: Expr, context: &FunctionContextInput<'_>) -> PlanResult<Expr> {
    let data_type = expr.get_type(context.schema)?;
    if data_type.is_string() {
        return Ok(
            ScalarUDF::from(SparkDate::new(!context.plan_config.ansi_mode)).call(vec![expr]),
        );
    }
    let expr = if let DataType::Timestamp(TimeUnit::Nanosecond, timezone) = &data_type {
        // Keep the day before the epoch when reducing a negative fractional timestamp.
        cast(
            timestamp_epoch_units(expr, 1_000)?,
            DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()),
        )
    } else {
        expr
    };
    let date = cast_temporal(
        expr,
        &data_type,
        DataType::Date32,
        &context.plan_config.session_timezone,
        false,
    )?;
    // Spark's Cast.forceNullable includes TIMESTAMP_NTZ to DATE.
    declare_nullable_result(
        date,
        matches!(data_type, DataType::Timestamp(_, None)),
        context.schema,
    )
}

fn date_field(input: ScalarFunctionInput, part: &str) -> PlanResult<Expr> {
    let argument = input.arguments.one()?;
    let data_type = argument.get_type(input.function_context.schema)?;
    let date = if data_type.is_string()
        || matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null
        ) {
        datetime_date(argument, &input.function_context)?
    } else {
        argument
    };
    Ok(integer_part(date, part))
}

fn date_udf(input: ScalarFunctionInput, udf: impl ScalarUDFImpl + 'static) -> PlanResult<Expr> {
    let types = input
        .arguments
        .iter()
        .map(|expr| expr.get_type(input.function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    udf.coerce_types(&types)?;
    let date = datetime_date(input.arguments.one()?, &input.function_context)?;
    Ok(ScalarUDF::from(udf).call(vec![date]))
}

fn time_field(input: ScalarFunctionInput, part: &str) -> PlanResult<Expr> {
    let mut timestamp = input.arguments.one()?;
    if timestamp
        .get_type(input.function_context.schema)?
        .is_string()
    {
        timestamp = datetime_timestamp(timestamp, &input.function_context, false, false)?;
    }
    Ok(integer_part(timestamp, part))
}

fn years(arg: Expr) -> Expr {
    integer_part(arg, "YEAR")
}

fn trunc_part_conversion(part: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![
            (
                Box::new(
                    part.clone()
                        .ilike(lit("mon"))
                        .or(part.clone().ilike(lit("mm"))),
                ),
                Box::new(lit("month")),
            ),
            (
                Box::new(
                    part.clone()
                        .ilike(lit("yy"))
                        .or(part.clone().ilike(lit("yyyy"))),
                ),
                Box::new(lit("year")),
            ),
            (
                Box::new(part.clone().ilike(lit("dd"))),
                Box::new(lit("day")),
            ),
        ],
        else_expr: Some(Box::new(part)),
    })
}

fn trunc(date: Expr, part: Expr) -> Expr {
    cast(
        ScalarUDF::from(SparkDateTrunc::new()).call(vec![trunc_part_conversion(part), date]),
        DataType::Date32,
    )
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    let part = trunc_part_conversion(part);
    // Spark skips the timestamp for invalid constant units, but generated execution evaluates
    // it for equivalent dynamic units. Keep ordinary argument evaluation in both cases: an
    // invalid unit returns NULL only after its non-NULL arguments evaluate successfully.
    let timestamp = datetime_timestamp(timestamp, &input.function_context, false, false)?;
    let truncated = ScalarUDF::from(SparkDateTrunc::new()).call(vec![part, timestamp]);
    let truncated = match truncated.get_type(input.function_context.schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, _) => truncated,
        DataType::Timestamp(_, tz) => {
            cast(truncated, DataType::Timestamp(TimeUnit::Microsecond, tz))
        }
        other => Err(PlanError::InternalError(format!(
            "date_trunc expected a timestamp result, got {other:?}"
        )))?,
    };
    Ok(truncated)
}

fn interval_arithmetic(input: ScalarFunctionInput, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = input.arguments.two()?;

    let interval = match unit.to_lowercase().as_str() {
        "years" | "year" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(years)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(years, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "years"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "months" | "month" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(months)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(0, months))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "months"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "days" | "day" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(days)), metadata) => Expr::Literal(
                ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(days, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "days"),
                DataType::Interval(IntervalUnit::DayTime),
            ),
        },
        _ => {
            return Err(PlanError::invalid(format!(
                "add_interval does not support interval unit type '{unit}'"
            )));
        }
    };
    let date = if matches!(
        date.get_type(input.function_context.schema)?,
        DataType::Timestamp(_, Some(_))
    ) {
        datetime_date(date, &input.function_context)?
    } else {
        cast(date, DataType::Date32)
    };
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(date),
        op,
        right: Box::new(interval),
    }))
}

fn format_interval(interval: Expr, unit: &str) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(interval),
        op: Operator::StringConcat,
        right: Box::new(lit(format!(" {unit}"))),
    })
}

fn timestampadd_with_name(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    let (unit, quantity, timestamp) = input.arguments.three()?;
    let unit = match &unit {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
        Expr::Column(col) => col.name().to_string(),
        _ => {
            return Err(PlanError::invalid(format!(
                "{function_name} unit must be a string literal or keyword"
            )));
        }
    };
    let unit = unit.trim().to_uppercase();
    if !matches!(
        unit.as_str(),
        "YEAR"
            | "QUARTER"
            | "MONTH"
            | "WEEK"
            | "DAY"
            | "DAYOFYEAR"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
            | "MICROSECOND"
    ) {
        return Err(PlanError::invalid(format!(
            "{function_name} does not support interval unit type '{unit}'"
        )));
    }
    let timestamp_type = timestamp.get_type(input.function_context.schema)?;
    let ntz = matches!(timestamp_type, DataType::Timestamp(_, None));
    let timestamp = if timestamp_type.is_numeric() {
        // Sail's numeric timestamp operands use microseconds, unlike to_timestamp.
        cast(
            timestamp,
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::clone(
                    &input.function_context.plan_config.session_timezone,
                )),
            ),
        )
    } else {
        datetime_timestamp(timestamp, &input.function_context, ntz, false)?
    };
    Ok(ScalarUDF::from(SparkTimestampAdd::new()).call(vec![
        timestamp,
        cast(quantity, DataType::Int64),
        lit(unit),
    ]))
}

fn timestampadd(input: ScalarFunctionInput) -> PlanResult<Expr> {
    timestampadd_with_name(input, "timestampadd")
}

fn dateadd(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    match input.arguments.len() {
        2 => interval_arithmetic(input, "days", Operator::Plus),
        3 => timestampadd_with_name(input, function_name),
        _ => Err(PlanError::invalid(format!(
            "{function_name} requires 2 or 3 arguments"
        ))),
    }
}

fn make_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (year, month, day) = input.arguments.three()?;
    let ansi = input.function_context.plan_config.ansi_mode;
    let args = [year, month, day]
        .into_iter()
        .map(|arg| constructor_cast(arg, DataType::Int32, &input.function_context))
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(ScalarUDF::from(SparkDate::new(!ansi)).call(args))
}

fn constructor_cast(
    arg: Expr,
    data_type: DataType,
    context: &FunctionContextInput<'_>,
) -> PlanResult<Expr> {
    let from = arg.get_type(context.schema)?;
    let nullable = from.is_string()
        || (data_type == DataType::Int32 && (from.is_floating() || from.is_decimal()));
    let result = if context.plan_config.ansi_mode {
        cast(arg, data_type)
    } else {
        try_cast(arg, data_type)
    };
    declare_nullable_result(result, nullable, context.schema)
}

fn date_days_arithmetic(dt1: Expr, dt2: Expr, op: Operator) -> Expr {
    let (dt1, dt2) = match (&dt1, &dt2) {
        (Expr::Literal(ScalarValue::Date32(_), _), Expr::Literal(ScalarValue::Date32(_), _)) => {
            (dt1, dt2)
        }
        _ => (cast(dt1, DataType::Date32), cast(dt2, DataType::Date32)),
    };
    let dt1 = cast(dt1, DataType::Int32);
    let dt2 = cast(dt2, DataType::Int32);
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dt1),
        op,
        right: Box::new(dt2),
    })
}

fn timestamp_micros(expr: Expr) -> Expr {
    cast(
        cast(expr, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Int64,
    )
}

fn timestamp_time_micros(expr: Expr) -> Expr {
    let remainder = timestamp_micros(expr) % lit(MICROS_PER_DAY);
    (remainder + lit(MICROS_PER_DAY)) % lit(MICROS_PER_DAY)
}

fn timestamp_months(expr: Expr) -> Expr {
    integer_part(expr.clone(), "YEAR") * lit(12) + integer_part(expr, "MONTH")
}

fn timestamp_day_time_is_before(left: Expr, right: Expr) -> Expr {
    let left_day = integer_part(left.clone(), "DAY");
    let right_day = integer_part(right.clone(), "DAY");
    left_day.clone().lt(right_day.clone()).or(left_day
        .eq(right_day)
        .and(timestamp_time_micros(left).lt(timestamp_time_micros(right))))
}

fn timestamp_month_diff(start: Expr, end: Expr) -> PlanResult<Expr> {
    let start = cast(start, DataType::Timestamp(TimeUnit::Microsecond, None));
    let end = cast(end, DataType::Timestamp(TimeUnit::Microsecond, None));
    let months = cast(
        timestamp_months(end.clone()) - timestamp_months(start.clone()),
        DataType::Int64,
    );
    let incomplete_positive_month = months
        .clone()
        .gt(lit(0_i64))
        .and(timestamp_day_time_is_before(end.clone(), start.clone()));
    let incomplete_negative_month = months
        .clone()
        .lt(lit(0_i64))
        .and(timestamp_day_time_is_before(start, end));
    Ok(when(incomplete_positive_month, months.clone() - lit(1_i64))
        .when(incomplete_negative_month, months.clone() + lit(1_i64))
        .otherwise(months)?)
}

fn timestampdiff_calendar_unit(unit: &str, start: Expr, end: Expr) -> PlanResult<Expr> {
    let months = timestamp_month_diff(start, end)?;
    match unit {
        "MONTH" => Ok(months),
        "QUARTER" => Ok(months / lit(3_i64)),
        "YEAR" => Ok(months / lit(12_i64)),
        _ => Err(PlanError::internal(format!(
            "invalid timestampdiff calendar unit: {unit}"
        ))),
    }
}

fn timestampdiff_fixed_unit(unit: &str, start: Expr, end: Expr) -> Expr {
    let divisor = match unit {
        "MILLISECOND" => 1_000i64,
        "SECOND" => 1_000_000i64,
        "MINUTE" => 60_000_000i64,
        "HOUR" => 3_600_000_000i64,
        "DAY" => 24 * 3_600_000_000i64,
        "WEEK" => 7 * 24 * 3_600_000_000i64,
        _ => 1i64,
    };
    ScalarUDF::from(SparkTimestampAdd::new_subtract(true)).call(vec![end, start, lit(divisor)])
}

fn datediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let args = input.arguments;
    match args.len() {
        2 => {
            let [start, end] = <[Expr; 2]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            let start = datetime_date(start, &input.function_context)?;
            let end = datetime_date(end, &input.function_context)?;
            Ok(date_days_arithmetic(start, end, Operator::Minus))
        }
        3 => {
            let [unit, start, end] = <[Expr; 3]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            let unit_str = match &unit {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.to_uppercase(),
                Expr::Column(col) => col.name().to_uppercase(),
                _ => {
                    return Err(PlanError::invalid(
                        "datediff unit must be a string literal or keyword",
                    ));
                }
            };
            let numeric = start.get_type(input.function_context.schema)?.is_numeric()
                || end.get_type(input.function_context.schema)?.is_numeric();
            // Preserve Sail's numeric day inputs to the three-argument datediff aliases.
            if unit_str == "DAY" && numeric {
                let days = |value: Expr| -> PlanResult<Expr> {
                    let date = if matches!(
                        value.get_type(input.function_context.schema)?,
                        DataType::Timestamp(_, Some(_))
                    ) {
                        datetime_date(value, &input.function_context)?
                    } else {
                        cast(value, DataType::Date32)
                    };
                    Ok(cast(date, DataType::Int64))
                };
                return Ok(days(end)? - days(start)?);
            }
            if matches!(
                unit_str.as_str(),
                "DAY" | "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND" | "WEEK"
            ) {
                let timestamp = |value| -> PlanResult<Expr> {
                    if numeric {
                        Ok(cast(
                            value,
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                        ))
                    } else {
                        datetime_timestamp(value, &input.function_context, false, false)
                    }
                };
                return Ok(timestampdiff_fixed_unit(
                    &unit_str,
                    timestamp(start)?,
                    timestamp(end)?,
                ));
            }
            // TimestampDiff first casts both operands to LTZ, then compares their
            // session-local date-times (including partial days and months).
            let local = |value: Expr| -> PlanResult<Expr> {
                if numeric {
                    return Ok(value);
                }
                let timezone = &input.function_context.plan_config.session_timezone;
                Ok(cast_temporal(
                    datetime_timestamp(value, &input.function_context, false, false)?,
                    &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone))),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    timezone,
                    false,
                )?)
            };
            let start = local(start)?;
            let end = local(end)?;
            match unit_str.as_str() {
                "MONTH" | "YEAR" | "QUARTER" => timestampdiff_calendar_unit(&unit_str, start, end),
                other => Err(PlanError::unsupported(format!("datediff unit: {other}"))),
            }
        }
        n => Err(PlanError::invalid(format!(
            "datediff requires 2 or 3 arguments, got {n}"
        ))),
    }
}

fn session_timezone(input: &ScalarFunctionInput) -> Expr {
    lit(input
        .function_context
        .plan_config
        .session_timezone
        .to_string())
}

fn current_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    input.arguments.zero()?;
    Ok(session_tz)
}

fn coerce_datetime_format(
    function_name: &str,
    format: Expr,
    schema: &DFSchemaRef,
) -> PlanResult<Expr> {
    let data_type = format.get_type(schema)?;
    if data_type.is_nested() {
        Err(PlanError::invalid(format!(
            "{function_name} format argument must be a string, got {data_type}"
        )))
    } else if data_type.is_string() {
        Ok(format)
    } else {
        Ok(cast(format, DataType::Utf8))
    }
}

fn declare_nullable_result(expr: Expr, nullable: bool, schema: &DFSchemaRef) -> PlanResult<Expr> {
    if nullable && !expr.nullable(schema)? {
        let null = lit(ScalarValue::try_from(&expr.get_type(schema)?)?);
        Ok(when(lit(true), expr).otherwise(null)?)
    } else {
        Ok(expr)
    }
}

fn to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let null_on_error = !input.function_context.plan_config.ansi_mode;
    if input.arguments.len() == 1 {
        // If format is not supplied, the function is a synonym for cast(expr AS DATE).
        let expr = input.arguments[0].clone();
        let data_type = expr.get_type(input.function_context.schema)?;
        if data_type.is_string() {
            Ok(ScalarUDF::from(SparkDate::new(null_on_error)).call(vec![expr]))
        } else {
            Ok(cast_temporal(
                expr,
                &data_type,
                DataType::Date32,
                &input.function_context.plan_config.session_timezone,
                false,
            )?)
        }
    } else if input.arguments.len() == 2 {
        let expr = input.arguments[0].clone();
        let format = coerce_datetime_format(
            "to_date",
            input.arguments[1].clone(),
            input.function_context.schema,
        )?;
        let expr_type = expr.get_type(input.function_context.schema);
        let date = match &expr_type {
            Ok(DataType::Timestamp(_, _)) => {
                Some(ScalarUDF::from(SparkToLocalTime::default()).call(vec![expr.clone()]))
            }
            Ok(DataType::Date32 | DataType::Date64) => Some(expr.clone()),
            _ => None,
        };
        if let Some(date) = date {
            let nullable = !input.function_context.plan_config.ansi_mode
                || expr.nullable(input.function_context.schema)?
                || format.nullable(input.function_context.schema)?;
            return declare_nullable_result(
                cast(date, DataType::Date32),
                nullable,
                input.function_context.schema,
            );
        }
        // Spark implicitly casts atomic inputs to string before formatted parsing.
        // Leave nested inputs to the parser's existing type rejection.
        let expr = match expr_type {
            Ok(data_type) if data_type.is_string() || data_type.is_nested() => expr,
            _ => cast(expr, DataType::Utf8),
        };
        Ok(ScalarUDF::from(SparkDate::new(null_on_error)).call(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_date requires 1 or 2 arguments"))
    }
}

fn try_to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    match input.arguments.len() {
        1 => {
            let value = input.arguments.one()?;
            let from = value.get_type(schema)?;
            if matches!(
                from,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            ) {
                return datetime_date(value, &input.function_context);
            }
            let value = if from.is_string() || from.is_nested() {
                value
            } else {
                cast(value, DataType::Utf8)
            };
            Ok(ScalarUDF::from(SparkDate::new(true)).call(vec![value]))
        }
        2 => {
            // Spark parses formatted dates as LTZ timestamps before taking the local date.
            // Reuse temporal-input handling and safe parsing from try_to_timestamp.
            let timezone = Arc::clone(&input.function_context.plan_config.session_timezone);
            let timestamp = timestamp_with_try(input, false, true, "try_to_date")?;
            Ok(cast_temporal(
                timestamp.clone(),
                &timestamp.get_type(schema)?,
                DataType::Date32,
                &timezone,
                false,
            )?)
        }
        _ => Err(PlanError::invalid("try_to_date requires 1 or 2 arguments")),
    }
}

fn validate_unix_timestamp_format(format: &Expr, schema: &DFSchemaRef) -> PlanResult<()> {
    match format.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => Ok(()),
        data_type => Err(PlanError::invalid(format!(
            "unix_timestamp format argument must be a string, got {data_type}"
        ))),
    }
}

fn unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let timezone = input.function_context.plan_config.session_timezone.clone();
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    if input.arguments.is_empty() {
        let expr = ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Second)).call(vec![]);
        Ok(cast(expr, DataType::Int64))
    } else if input.arguments.len() == 1 {
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode)).call(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        if matches!(
            expr.get_type(input.function_context.schema)?,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null
        ) {
            validate_unix_timestamp_format(&format, input.function_context.schema)?;
            let nullable = !ansi_mode
                || expr.nullable(input.function_context.schema)?
                || format.nullable(input.function_context.schema)?;
            let result =
                ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode)).call(vec![expr]);
            declare_nullable_result(result, nullable, input.function_context.schema)
        } else {
            Ok(
                ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode))
                    .call(vec![expr, format]),
            )
        }
    } else {
        Err(PlanError::invalid(
            "unix_timestamp requires 0, 1, or 2 arguments",
        ))
    }
}

fn to_unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if !(1..=2).contains(&input.arguments.len()) {
        Err(PlanError::invalid(
            "to_unix_timestamp requires 1 or 2 arguments",
        ))
    } else {
        unix_timestamp(input)
    }
}

/// Dispatch for `next_day(date, day_of_week)`.
///
/// Reads `PlanConfig::ansi_mode` at planning time and bakes it into the UDF
/// so the runtime path chooses between erroring (ANSI=true) and returning
/// NULL (ANSI=false) on malformed day-of-week strings.
fn next_day(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkNextDay::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

pub(super) fn date_format(expr: Expr, format: Expr, timezone: String) -> Expr {
    date_format_with_args(vec![expr, format], timezone)
}

fn date_format_with_args(arguments: Vec<Expr>, timezone: String) -> Expr {
    ScalarUDF::from(SparkDateFormat::new(timezone.into())).call(arguments)
}

fn timestamp_data_type(input: &ScalarFunctionInput, timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn can_fold_datetime_arguments<'a>(
    arguments: impl IntoIterator<Item = &'a Expr>,
) -> PlanResult<bool> {
    // Aggregates and generators are extracted after scalar function resolution.
    // Removing either here would change the number of output rows.
    for argument in arguments {
        if argument.exists(|expr| {
            Ok(match expr {
                Expr::AggregateFunction(_) => true,
                Expr::ScalarFunction(function) => function.func.inner().is::<Explode>(),
                _ => false,
            })
        })? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool, name: &str) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, false, name)
}

fn to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, false)
}

fn try_to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, true)
}

/// Shared `to_time` / `try_to_time` planner, including Sail's temporal inputs.
/// `to_time` errors on failure (Spark's `ToTime` is ANSI-invariant); `try_to_time`
/// (`is_try`) returns NULL.
fn time_with_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let udf = ScalarUDF::from(SparkTime::new(is_try));
    if !matches!(input.arguments.len(), 1 | 2) {
        let name = if is_try { "try_to_time" } else { "to_time" };
        return Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )));
    }
    declare_nullable_result(
        cast(
            udf.call(input.arguments),
            DataType::Time64(TimeUnit::Microsecond),
        ),
        true,
        input.function_context.schema,
    )
}

fn try_to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, true, "try_to_timestamp")
}

/// Shared `to_timestamp` / `try_to_timestamp` (+ `_ntz`) planner.
///
fn datetime_timestamp(
    expression: Expr,
    context: &FunctionContextInput<'_>,
    ntz: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let from = expression.get_type(context.schema)?;
    let timezone = (!ntz).then(|| Arc::clone(&context.plan_config.session_timezone));
    if from.is_numeric() && !ntz {
        let mode = if is_try {
            TimestampNumberMode::TryCast
        } else if context.plan_config.ansi_mode {
            TimestampNumberMode::CastAnsi
        } else {
            TimestampNumberMode::CastLegacy
        };
        Ok(cast(
            ScalarUDF::from(SparkTimestampFromNumber::new(mode)).call(vec![expression]),
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
        ))
    } else if from.is_string()
        || (ntz && from.is_numeric())
        || matches!(
            from,
            DataType::Boolean | DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
    {
        let expression = if from.is_string() {
            expression
        } else {
            cast(expression, DataType::Utf8)
        };
        Ok(ScalarUDF::from(SparkTimestamp::try_new(
            timezone,
            context.plan_config.ansi_mode,
            is_try,
        )?)
        .call(vec![expression]))
    } else {
        Ok(cast_temporal(
            expression,
            &from,
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
            &context.plan_config.session_timezone,
            is_try,
        )?)
    }
}

/// The 1-arg form goes through `cast` / `try_cast`, which route strings to
/// `SparkTimestamp` (honoring ANSI for the strict variant) and cast other types.
/// The 2-arg form parses the value with the given format via `SparkTimestamp`.
fn timestamp_with_try(
    input: ScalarFunctionInput,
    timestamp_ntz: bool,
    is_try: bool,
    name: &str,
) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        datetime_timestamp(expr, &input.function_context, timestamp_ntz, is_try)
    } else if input.arguments.len() == 2 {
        let (value, format) = input.arguments.two()?;
        let format = coerce_datetime_format("to_timestamp", format, input.function_context.schema)?;
        let source = value.get_type(input.function_context.schema)?;
        let value = if matches!(
            source,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        ) {
            // Spark GetTimestamp ignores the format for temporal inputs and
            // preserves timestamp microseconds, even when the output flavor changes.
            let instant = if matches!(source, DataType::Date32 | DataType::Date64) {
                datetime_timestamp(value, &input.function_context, false, false)?
            } else {
                cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))
            };
            // Keep both arguments through analysis and ordinary execution so formats
            // cannot erase required aggregates, windows or subqueries. Computing a format
            // can raise even though a typed temporal value needs no format parser; Sail
            // deliberately does not reproduce Spark's optimizer-dependent suppression.
            cast(cast(instant, DataType::Int64), data_type.clone())
        } else if source.is_string() || source.is_nested() {
            value
        } else {
            cast(value, DataType::Utf8)
        };
        let arguments = vec![value, format];
        Ok(cast(
            ScalarUDF::from(SparkTimestamp::try_new(
                match &data_type {
                    DataType::Timestamp(_, timezone) => timezone.clone(),
                    _ => None,
                },
                ansi_mode,
                is_try,
            )?)
            .call(arguments),
            data_type,
        ))
    } else {
        Err(wrong_datetime_arity(name, "[1, 2]", input.arguments.len()))
    }
}

fn from_unixtime(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (expr, format) = if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        // default format is "yyyy-MM-dd HH:mm:ss"
        Ok((expr, lit("yyyy-MM-dd HH:mm:ss")))
    } else if input.arguments.len() == 2 {
        input.arguments.two()
    } else {
        return Err(wrong_datetime_arity(
            "from_unixtime",
            "[1, 2]",
            input.arguments.len(),
        ));
    }?;

    let timezone = input.function_context.plan_config.session_timezone.clone();
    let expr = cast(
        expr,
        DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
    );
    // Spark's FromUnixTime declares a nullable result independently of its inputs.
    declare_nullable_result(
        date_format(expr, format, timezone.to_string()),
        true,
        input.function_context.schema,
    )
}

fn unix_time_unit(input: ScalarFunctionInput, time_unit: TimeUnit) -> PlanResult<Expr> {
    let arg = input.arguments.one()?;
    let micros = cast(
        cast(
            arg,
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(input.function_context.plan_config.session_timezone.clone()),
            ),
        ),
        DataType::Int64,
    );
    let divisor = 1_000_000 / time_unit_to_multiplier(&time_unit);
    Ok(timestamp_epoch_units(micros, divisor)?)
}

fn current_timestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
        let timezone = input.function_context.plan_config.session_timezone.clone();
        Ok(ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Microsecond)).call(vec![]))
    } else {
        Err(PlanError::invalid(format!(
            "current_timestamp takes 0 arguments, got {:?}",
            input.arguments
        )))
    }
}

fn current_localtimestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let expr = current_timestamp_microseconds(input)?;
    Ok(ScalarUDF::from(SparkToLocalTime::default()).call(vec![expr]))
}

fn current_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    input.arguments.zero()?;
    let timezone = input.function_context.plan_config.session_timezone.clone();
    let timestamp =
        ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Microsecond)).call(vec![]);
    Ok(cast(
        ScalarUDF::from(SparkToLocalTime::default()).call(vec![timestamp]),
        DataType::Date32,
    ))
}

fn convert_tz(
    from_tz: Expr,
    to_tz: Expr,
    ts: Expr,
    classic: bool,
    with_null_short_circuit: bool,
) -> PlanResult<Expr> {
    let func = ConvertTz::new(classic);
    if with_null_short_circuit {
        let arguments = [from_tz, to_tz, ts]
            .into_iter()
            .map(|argument| lambda_with_fresh_parameter(argument, "_convert_tz"))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(Expr::HigherOrderFunction(expr::HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(
                func.with_null_short_circuit(),
            ))),
            arguments,
        )))
    } else {
        Ok(ScalarUDF::from(func).call(vec![from_tz, to_tz, ts]))
    }
}

fn validate_convert_timezone_zone(expr: &Expr, schema: &DFSchemaRef) -> PlanResult<()> {
    let field = expr.to_field(schema)?.1;
    if !can_cast_types(field.data_type(), &DataType::Utf8)
        && !is_variant_storage_field(field.as_ref())
    {
        return Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] convert_timezone time zone arguments must be castable to string, got {}",
            field.data_type()
        )));
    }
    Ok(())
}

fn has_unsupported_datetime_casts<'a>(
    arguments: impl IntoIterator<Item = &'a Expr>,
    schema: &DFSchemaRef,
) -> PlanResult<bool> {
    for argument in arguments {
        if argument.exists(|expr| {
            match expr {
                Expr::Cast(cast) => {
                    return Ok(!can_cast_types(
                        &cast.expr.get_type(schema)?,
                        cast.field.data_type(),
                    ));
                }
                Expr::TryCast(cast) => {
                    return Ok(!can_cast_types(
                        &cast.expr.get_type(schema)?,
                        cast.field.data_type(),
                    ));
                }
                Expr::ScalarFunction(function)
                    if function.func.inner().is::<SparkCastToVariant>() =>
                {
                    for argument in &function.args {
                        if SparkCastToVariant::validate_input_type(&argument.get_type(schema)?)
                            .is_err()
                        {
                            return Ok(true);
                        }
                    }
                }
                _ => {}
            }
            Ok(false)
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_foldable_timezone_argument(argument: &Expr) -> PlanResult<bool> {
    fn has_non_foldable_node(argument: &Expr) -> datafusion_common::Result<bool> {
        let mut found = false;
        argument.apply(|expr| {
            if let Expr::HigherOrderFunction(function) = expr
                && (function.func.inner().as_ref() as &dyn std::any::Any).is::<ConvertTzLazy>()
            {
                // These lambdas lower a scalar SQL function, not a SQL higher-order function.
                for argument in &function.args {
                    let Expr::Lambda(lambda) = argument else {
                        found = true;
                        return Ok(TreeNodeRecursion::Stop);
                    };
                    if has_non_foldable_node(&lambda.body)? {
                        found = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            found = match expr {
                Expr::ScalarFunction(function) => {
                    function.func.signature().volatility != Volatility::Immutable
                }
                Expr::HigherOrderFunction(_)
                | Expr::ScalarVariable(_, _)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::Exists { .. }
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::ScalarSubquery(_)
                | Expr::WindowFunction(_)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_)
                | Expr::Placeholder(_)
                | Expr::Lambda(_)
                | Expr::LambdaVariable(_) => true,
                _ => false,
            };
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(found)
    }

    Ok(!(argument.any_column_refs()
        || !can_fold_datetime_arguments([argument])?
        || has_non_foldable_node(argument)?
        || expr_contains_python_udf(argument)?
        || expr_contains_spark_cast_to_variant(argument)?))
}

pub(super) fn fold_datetime_argument(argument: Expr, schema: &DFSchemaRef) -> PlanResult<Expr> {
    if !is_foldable_timezone_argument(&argument)? {
        return Ok(argument);
    }
    // Stable functions such as current_timestamp require the query's execution context.
    let context = SimplifyContextBuilder::default()
        .with_schema(Arc::clone(schema))
        .build();
    let simplifier = ExprSimplifier::new(context);
    let argument = simplifier.coerce(argument, schema)?;
    // Keep normal simplification, including NULL propagation, without forcing deferred
    // errors. Spark's NullPropagation/ConstantFolding ordering can raise for a computed
    // NULL where a literal NULL suppresses the same argument. Sail leaves surviving
    // expressions to ordinary execution and convert_timezone's left-to-right NULL checks.
    Ok(simplifier.simplify(argument)?)
}

/// A helper function for processing the input NTZ timestamp.
fn ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok((
            ScalarUDF::from(SparkToLocalTime::default()).call(vec![ts]),
            unit,
        )),
        DataType::Timestamp(unit, None) => Ok((ts, unit)),
        DataType::Null => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts =
                ScalarUDF::from(SparkTimestamp::try_new(None, ansi_mode, false)?).call(vec![ts]);
            Ok((ts, unit))
        }
        x => Err(PlanError::invalid(format!(
            "invalid NTZ timestamp type: {x:?}"
        ))),
    }
}

fn convert_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let args = input.arguments;
    let (from_tz, to_tz, ts) = match args.len() {
        3 => Ok(args.three()?),
        2 => {
            let (to_tz, ts) = args.two()?;
            Ok((lit(session_tz.to_string()), to_tz, ts))
        }
        _ => Err(PlanError::invalid(format!(
            "convert_timezone takes 2 or 3 arguments, got {args:?}"
        ))),
    }?;
    validate_convert_timezone_zone(&from_tz, input.function_context.schema)?;
    validate_convert_timezone_zone(&to_tz, input.function_context.schema)?;
    let (ts, _) = ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        input.function_context.plan_config.ansi_mode,
    )?;
    if has_unsupported_datetime_casts([&from_tz, &to_tz, &ts], input.function_context.schema)? {
        // Preserve native cast validation and errors from reached Variant casts, while
        // retaining Sail's support for unevaluated CASE/COALESCE branches.
        return Ok(
            ScalarUDF::from(ConvertTz::new(true).with_null_short_circuit())
                .call(vec![from_tz, to_tz, ts]),
        );
    }
    let requires_scalar_evaluation =
        [&from_tz, &to_tz, &ts]
            .into_iter()
            .try_fold(false, |found, argument| {
                if found {
                    PlanResult::Ok(true)
                } else {
                    Ok(expr_contains_python_udf(argument)?)
                }
            })?;

    if requires_scalar_evaluation {
        Ok(
            ScalarUDF::from(ConvertTz::new(true).with_null_short_circuit())
                .call(vec![from_tz, to_tz, ts]),
        )
    } else {
        convert_tz(from_tz, to_tz, ts, true, true)
    }
}

/// A helper function for processing the input timestamp for
/// `from_utc_timestamp` and `to_utc_timestamp` functions.
/// These functions expect timestamps with time zone, but consider the value
/// relative to the UTC time zone.
fn utc_ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    session_tz: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    let (ts, unit) = match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => (ts, unit),
        DataType::Timestamp(unit, None) => {
            let ts = cast_temporal(
                ts,
                &DataType::Timestamp(unit, None),
                DataType::Timestamp(unit, Some(session_tz.clone())),
                session_tz,
                false,
            )?;
            (ts, unit)
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            let from = ts.get_type(schema)?;
            let ts = cast_temporal(
                ts,
                &from,
                DataType::Timestamp(unit, Some(session_tz.clone())),
                session_tz,
                false,
            )?;
            (ts, unit)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts = ScalarUDF::from(SparkTimestamp::try_new(
                Some(session_tz.clone()),
                ansi_mode,
                false,
            )?)
            .call(vec![ts]);
            (ts, unit)
        }
        x => {
            return Err(PlanError::invalid(format!(
                "invalid UTC NTZ timestamp type: {x:?}"
            )));
        }
    };
    let ts = cast(ts, DataType::Timestamp(unit, None));
    Ok((ts, unit))
}

fn utc_timestamp(input: ScalarFunctionInput, from_utc: bool) -> PlanResult<Expr> {
    let session_tz = Arc::clone(&input.function_context.plan_config.session_timezone);
    let (ts, zone) = input.arguments.two()?;
    let zone_type = zone.get_type(input.function_context.schema)?;
    // NULL-first zone parsing does not bypass static types. Spark coerces atomic values
    // to strings here, but not containers; explicit casts to strings remain supported.
    if zone_type.is_nested() || !can_cast_types(&zone_type, &DataType::Utf8) {
        return Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] UTC conversion time zone argument must be castable to string, got {zone_type}"
        )));
    }
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let output_type = DataType::Timestamp(unit, Some(session_tz));
    // Spark's generated code validates constant zones before checking a NULL timestamp,
    // while dynamic zones and interpreted execution check NULL first. Always check NULL
    // before parsing zone values; non-NULL conversions still reject invalid time zones.
    let func = ConvertTz::new(false).with_null_short_circuit();
    let (from, to) = if from_utc {
        (lit("UTC"), zone)
    } else {
        (zone, lit("UTC"))
    };
    let ts = ScalarUDF::from(func).call(vec![from, to, ts]);
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, output_type))
}

fn make_timestamp(
    input: ScalarFunctionInput,
    explicit_ntz: Option<bool>,
    is_try: bool,
) -> PlanResult<Expr> {
    let config = input.function_context.plan_config;
    let ntz = explicit_ntz.unwrap_or(matches!(
        config.default_timestamp_type,
        DefaultTimestampType::TimestampNtz
    ));
    let mut args = input.arguments;
    if explicit_ntz.is_none() && args.len() == 1 {
        args.push(lit(ScalarValue::Time64Microsecond(Some(0))));
    }
    let zone = if explicit_ntz != Some(true) && matches!(args.len(), 3 | 7) {
        args.pop()
    } else {
        None
    };
    if !matches!(args.len(), 2 | 6) {
        let (name, arities) = if ntz {
            ("make_timestamp_ntz", "2 or 6")
        } else {
            ("make_timestamp_ltz", "2, 3, 6 or 7")
        };
        return Err(PlanError::invalid(format!(
            "{}{name} requires {arities} arguments, got {args:?}",
            if is_try { "try_" } else { "" },
        )));
    }
    let safe = is_try || !config.ansi_mode;
    let udf = SparkMakeTimestampNtz::new(safe, config.ansi_mode);
    let types = args
        .iter()
        .map(|arg| arg.get_type(input.function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let coerced_types = udf.coerce_types(&types)?;
    let args = args
        .into_iter()
        .zip(types)
        .zip(coerced_types)
        .map(|((arg, from), to)| {
            if from == to {
                Ok(arg)
            } else if is_try
                && to == DataType::UInt32
                && matches!(from, DataType::Int8 | DataType::Int16 | DataType::Int32)
            {
                // Only negative components can fail this cast; preserve overflow errors
                // when converting wider inputs.
                Ok(try_cast(arg, to))
            } else {
                constructor_cast(arg, to, &input.function_context)
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let timestamp = ScalarUDF::from(udf).call(args);
    if ntz {
        Ok(timestamp)
    } else if let Some(zone) = zone {
        let instant = convert_tz(zone, lit("UTC"), timestamp, false, false)?;
        Ok(cast(
            cast(instant, DataType::Int64),
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::clone(&config.session_timezone)),
            ),
        ))
    } else {
        Ok(cast_timestamp_ntz_to_ltz(
            timestamp,
            Arc::clone(&config.session_timezone),
            safe,
        )?)
    }
}

fn date_part(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, mut source) = input.arguments.two()?;
    let context = input.function_context;
    let part_type = part.get_type(context.schema)?;
    if !part_type.is_string() && part_type != DataType::Null {
        return Err(PlanError::invalid(
            "date_part requires a foldable string field",
        ));
    }
    let part = fold_datetime_argument(part, context.schema)?;
    // Spark resolves EXTRACT's field during analysis; unlike timezone arguments,
    // its value is required here, so a failed constant expression must still raise.
    let part = if !matches!(part, Expr::Literal(_, _)) && is_foldable_timezone_argument(&part)? {
        lit(LiteralEvaluator::new().evaluate(&part)?)
    } else {
        part
    };
    let Expr::Literal(value, _) = &part else {
        return Err(PlanError::invalid(
            "date_part requires a foldable string field",
        ));
    };
    if value.is_null() {
        let source = ExprSimplifier::new(
            SimplifyContextBuilder::default()
                .with_schema(Arc::clone(context.schema))
                .build(),
        )
        .coerce(source, context.schema)?;
        // Spark's Extract replacement discards the source, including aggregates,
        // after type checking, without evaluating its values.
        if has_unsupported_datetime_casts([&source], context.schema)? {
            return Ok(ScalarUDF::from(SparkDatePart::new())
                .call(vec![lit(ScalarValue::Utf8(None)), source]));
        }
        return Ok(lit(ScalarValue::Float64(None)));
    }
    let name = value
        .try_as_str()
        .flatten()
        .ok_or_else(|| PlanError::invalid("date_part requires a foldable string field"))?
        .to_ascii_uppercase();
    let (part, date_field, sunday_one) = match name.as_str() {
        "YEAR" | "Y" | "YEARS" | "YR" | "YRS" => ("year", true, false),
        "YEAROFWEEK" | "ISOYEAR" => ("isoyear", true, false),
        "QUARTER" | "QTR" => ("quarter", true, false),
        "MONTH" | "MON" | "MONS" | "MONTHS" => ("month", true, false),
        "WEEK" | "W" | "WEEKS" => ("week", true, false),
        "DAY" | "D" | "DAYS" => ("day", true, false),
        "DAYOFWEEK" | "DOW" => ("dow", true, true),
        "DAYOFWEEK_ISO" | "DOW_ISO" | "ISODOW" => ("isodow", true, false),
        "DOY" => ("doy", true, false),
        "HOUR" | "H" | "HOURS" | "HR" | "HRS" => ("hour", false, false),
        "MINUTE" | "M" | "MIN" | "MINS" | "MINUTES" => ("minute", false, false),
        "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => ("second", false, false),
        // Keep the native aliases and additional parts accepted by Sail.
        _ => (name.as_str(), false, false),
    };
    let source_type = source.get_type(context.schema)?;
    if source_type.is_string() || source_type == DataType::Null {
        source = if date_field {
            datetime_date(source, &context)?
        } else {
            datetime_timestamp(source, &context, false, false)?
        };
    } else if let DataType::Timestamp(unit, Some(_)) = &source_type {
        // Native extraction already reads calendar fields; only the session-zone
        // metadata needs changing, without an extra timestamp-to-date buffer pass.
        source = cast(
            source,
            DataType::Timestamp(
                *unit,
                Some(Arc::clone(&context.plan_config.session_timezone)),
            ),
        );
    }
    // Native YEAR preimages can introduce NULL bounds when January 1 midnight is nonexistent.
    let result = ScalarUDF::from(SparkDatePart::new()).call(vec![lit(part), source]);
    let result = if sunday_one {
        result + lit(1i32)
    } else {
        result
    };
    // Spark's NTZ-to-date cast and TIME StaticInvoke expressions are nullable.
    declare_nullable_result(
        result,
        (date_field && matches!(source_type, DataType::Timestamp(_, None)))
            || (matches!(source_type, DataType::Time32(_) | DataType::Time64(_))
                && matches!(part, "hour" | "minute" | "second")),
        context.schema,
    )
}

fn months_between(input: ScalarFunctionInput) -> PlanResult<Expr> {
    // args extraction:
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let round_off = (arguments.len() == 3)
        .then(|| arguments.pop())
        .flatten()
        .unwrap_or(lit(true));
    let (date1, date2) = arguments.two()?;

    // consts:
    let seconds_per_day: i64 = 24 * 60 * 60;
    let seconds_in_month = cast(lit(31 * seconds_per_day), DataType::Float64);

    // helper functions:

    let date_to_months =
        |dt: Expr| integer_part(dt.clone(), "YEAR") * lit(12) + integer_part(dt, "MONTH");

    let is_last_day = |dt: Expr| {
        let dt = ScalarUDF::from(SparkToLocalTime::default()).call(vec![dt]);
        ScalarUDF::from(SparkLastDay::new())
            .call(vec![cast(dt.clone(), DataType::Date32)])
            .eq(cast(dt, DataType::Date32))
    };

    let seconds_in_day = |dt: Expr, tu: TimeUnit| {
        (cast(dt.clone(), DataType::Int64)
            - cast(
                ScalarUDF::from(SparkStartOfDay::new()).call(vec![dt]),
                DataType::Int64,
            ))
            / lit(time_unit_to_multiplier(&tu))
    };

    // prerequisites
    let numeric = date1.get_type(function_context.schema)?.is_numeric()
        || date2.get_type(function_context.schema)?.is_numeric();
    let timestamp = |value: Expr| -> PlanResult<Expr> {
        if numeric {
            // Preserve the existing timezone flavors when using Sail's numeric operands.
            let timezone = match value.get_type(function_context.schema)? {
                DataType::Timestamp(_, timezone) => timezone,
                _ => None,
            };
            Ok(cast(
                value,
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
            ))
        } else {
            datetime_timestamp(value, &function_context, false, false)
        }
    };
    let date1 = timestamp(date1)?;
    let date2 = timestamp(date2)?;

    // calculations:
    let days1 = integer_part(date1.clone(), "DAY");
    let days2 = integer_part(date2.clone(), "DAY");

    let month_diff = cast(
        date_to_months(date1.clone()) - date_to_months(date2.clone()),
        DataType::Float64,
    );

    let seconds_diff = (days1.clone() - days2.clone()) * lit(seconds_per_day)
        + seconds_in_day(date1.clone(), TimeUnit::Microsecond)
        - seconds_in_day(date2.clone(), TimeUnit::Microsecond);

    let months_between = when(
        days1
            .eq(days2)
            .or(is_last_day(date1).and(is_last_day(date2))),
        month_diff.clone(),
    )
    .otherwise(month_diff + seconds_diff / seconds_in_month)?;

    let result = when(
        round_off.clone(),
        expr_fn::round(vec![months_between.clone(), lit(8)]),
    )
    .otherwise(months_between)?;
    if round_off.nullable(function_context.schema)? {
        Ok(when(round_off.is_null(), lit(ScalarValue::Float64(None))).otherwise(result)?)
    } else {
        Ok(result)
    }
}

// Parses window durations, including Sail's interval literal extension.
fn window_interval_micros(expr: &Expr) -> PlanResult<i64> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlanError::invalid(
            "The duration and time inputs to window must be an integer, long or string literal. \
             Sail additionally accepts day-time interval literals.",
        ));
    };
    if let Some(s) = value.try_as_str().flatten() {
        return match parse_interval(s)
            .map_err(|e| PlanError::invalid(format!("invalid window interval {s:?}: {e}")))?
        {
            IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
            _ => Err(PlanError::invalid(format!(
                "Intervals greater than a month is not supported ({s})."
            ))),
        };
    }
    match value {
        // Spark interprets integer literals as microseconds.
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::DurationMicrosecond(Some(v)) => Ok(*v),
        ScalarValue::DurationMillisecond(Some(v)) => v
            .checked_mul(1_000)
            .ok_or_else(|| PlanError::invalid("window interval overflow")),
        ScalarValue::DurationSecond(Some(v)) => v
            .checked_mul(1_000_000)
            .ok_or_else(|| PlanError::invalid("window interval overflow")),
        ScalarValue::DurationNanosecond(Some(v)) => Ok(*v / 1_000),
        ScalarValue::IntervalDayTime(Some(v)) => i64::try_from(
            i128::from(v.days) * i128::from(MICROS_PER_DAY) + i128::from(v.milliseconds) * 1_000,
        )
        .map_err(|_| PlanError::invalid("window interval overflow")),
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.months == 0 => i64::try_from(
            i128::from(v.days) * i128::from(MICROS_PER_DAY) + i128::from(v.nanoseconds / 1_000),
        )
        .map_err(|_| PlanError::invalid("window interval overflow")),
        _ => Err(PlanError::invalid(
            "window durations and start time must be literal strings, day-time intervals, or integers",
        )),
    }
}

/// The parsed durations (in microseconds) of a Spark `window` call.
#[derive(Debug, Clone, Copy)]
struct WindowSpec {
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

/// Bound on `ceil(windowDuration / slideDuration)`
const MAX_OVERLAPPING_WINDOWS: i64 = 1_000_000;

fn invalid_window_duration(name: &str, duration: i64) -> PlanError {
    PlanError::invalid(format!(
        "[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE] \
         The `{name}` must be between (0, {}] (current value = {duration}L).",
        i64::MAX
    ))
}

/// Parses and validates the `window` durations from the full argument list
/// (`args[0]` is the time column; `args[1..]` are window/slide/start).
fn parse_window_spec(args: &[Expr]) -> PlanResult<WindowSpec> {
    if !(2..=4).contains(&args.len()) {
        return Err(wrong_datetime_arity("window", "[2, 3, 4]", args.len()));
    }
    let window_duration = window_interval_micros(&args[1])?;
    let slide_duration = match args.get(2) {
        Some(arg) => window_interval_micros(arg)?,
        None => window_duration,
    };
    let start_time = match args.get(3) {
        Some(arg) => window_interval_micros(arg)?,
        None => 0,
    };
    if window_duration <= 0 {
        return Err(invalid_window_duration("window_duration", window_duration));
    }
    if slide_duration <= 0 {
        return Err(invalid_window_duration("slide_duration", slide_duration));
    }
    if slide_duration > window_duration {
        return Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION] \
             The `slide_duration`({slide_duration}L) must be <= the `window_duration`({window_duration}L)."
        )));
    }
    if start_time >= slide_duration || start_time <= -slide_duration {
        return Err(PlanError::invalid(format!(
            "The `abs(start_time)`({start_time}L) must be < the `slide_duration`({slide_duration}L)."
        )));
    }
    let overlapping = (window_duration + slide_duration - 1) / slide_duration;
    if overlapping > MAX_OVERLAPPING_WINDOWS {
        return Err(PlanError::invalid(format!(
            "window: ceil(windowDuration / slideDuration) = {overlapping} exceeds the limit of {MAX_OVERLAPPING_WINDOWS}"
        )));
    }
    Ok(WindowSpec {
        window_duration,
        slide_duration,
        start_time,
    })
}

/// The `window` struct field type: a microsecond timestamp (non-timestamp inputs
/// are cast, matching Spark's cast of the time column to `TimestampType`).
fn window_field_type(
    time_type: &DataType,
    session_tz: &std::sync::Arc<str>,
    time_name: &str,
) -> PlanResult<DataType> {
    Ok(match time_type {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        // Spark casts dates and strings to `TimestampType` (session time zone), so a
        // date becomes midnight in the session time zone, not a naive timestamp.
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone()))
        }
        other => {
            let input_type = SparkPlanFormatter
                .data_type_to_simple_string(other)?
                .to_uppercase();
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] \
                 The first parameter requires the \"TIMESTAMP\" or \"TIMESTAMP_NTZ\" type, \
                 however \"{time_name}\" has the type \"{input_type}\"."
            )));
        }
    })
}

/// The Spark `window` time function: buckets a timestamp into `struct<start, end>`
/// windows. The candidate enumeration is deferred to the `SparkWindowBuckets` UDF
/// so the plan stays bounded regardless of the `window/slide` ratio.
fn window(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let args = input.arguments;
    let spec = parse_window_spec(&args)?;
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let time = args
        .into_iter()
        .next()
        .ok_or_else(|| PlanError::internal("window missing time column"))?;
    let field_type = window_field_type(
        &time.get_type(schema)?,
        &session_tz,
        input
            .function_context
            .argument_display_names
            .first()
            .map(String::as_str)
            .unwrap_or("time column"),
    )?;
    let time_ts = cast(time, field_type);
    let buckets = ScalarUDF::from(SparkWindowBuckets::new(
        spec.window_duration,
        spec.slide_duration,
        spec.start_time,
    ))
    .call(vec![time_ts]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Explode)).call(vec![buckets]))
}

/// The Spark `window_time` function: the event-time of a time window, defined as
/// `window.end - 1 microsecond`. Spark validates the argument via column metadata
/// markers; we approximate that with a structural check on the window struct type.
fn window_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let arg = input.arguments.one()?;
    let end_type = match arg.get_type(schema)? {
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "start"
                && fields[1].name() == "end"
                && matches!(
                    fields[0].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                )
                && matches!(
                    fields[1].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                ) =>
        {
            fields[1].data_type().clone()
        }
        other @ DataType::Struct(_) => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {other:?}"
            )));
        }
        other => {
            let input_type = SparkPlanFormatter
                .data_type_to_simple_string(&other)?
                .to_uppercase();
            let input_name = input
                .function_context
                .argument_display_names
                .first()
                .map(String::as_str)
                .unwrap_or("argument");
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] \
                 The first parameter requires the \"STRUCT\" type, \
                 however \"{input_name}\" has the type \"{input_type}\"."
            )));
        }
    };
    Ok(cast(
        cast(arg.field("end"), DataType::Int64) - lit(1_i64),
        end_type,
    ))
}

pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|input| interval_arithmetic(input, "years", Operator::Plus)),
        ),
        (
            "add_months",
            F::custom(|input| interval_arithmetic(input, "months", Operator::Plus)),
        ),
        (
            "add_days",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("convert_timezone", F::custom(convert_timezone)),
        ("curdate", F::custom(current_date)),
        ("current_date", F::custom(current_date)),
        ("current_time", F::nullary(expr_fn::current_time)),
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        ("date_add", F::custom(|input| dateadd(input, "date_add"))),
        ("date_diff", F::custom(datediff)),
        (
            "date_format",
            F::custom(|input| match input.arguments.len() {
                2 => {
                    let timezone = input.function_context.plan_config.session_timezone.clone();
                    let (value, format) = input.arguments.two()?;
                    let data_type = value.get_type(input.function_context.schema)?;
                    let value = if data_type.is_string()
                        || matches!(
                            data_type,
                            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64
                        ) {
                        datetime_timestamp(value, &input.function_context, false, false)?
                    } else {
                        value
                    };
                    Ok(date_format_with_args(
                        vec![value, format],
                        timezone.to_string(),
                    ))
                }
                actual => Err(wrong_datetime_arity("date_format", "2", actual)),
            }),
        ),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::custom(date_part)),
        (
            "date_sub",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Minus)),
        ),
        ("date_trunc", F::custom(date_trunc)),
        ("dateadd", F::custom(|input| dateadd(input, "dateadd"))),
        ("datediff", F::custom(datediff)),
        ("datepart", F::custom(date_part)),
        ("day", F::custom(|input| date_field(input, "DAY"))),
        ("dayname", F::unary(|arg| expr_fn::to_char(arg, lit("%a")))),
        ("dayofmonth", F::custom(|input| date_field(input, "DAY"))),
        (
            "dayofweek",
            F::custom(|input| Ok(date_field(input, "DOW")? + lit(1))),
        ),
        ("dayofyear", F::custom(|input| date_field(input, "DOY"))),
        ("extract", F::custom(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        (
            "from_utc_timestamp",
            F::custom(|input| utc_timestamp(input, true)),
        ),
        ("hour", F::custom(|input| time_field(input, "HOUR"))),
        (
            "last_day",
            F::custom(|input| date_udf(input, SparkLastDay::new())),
        ),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::custom(make_date)),
        (
            "make_dt_interval",
            F::custom(|input| {
                Ok(ScalarUDF::from(
                    SparkMakeDtInterval::new()
                        .with_ansi_mode(input.function_context.plan_config.ansi_mode),
                )
                .call(input.arguments))
            }),
        ),
        (
            "make_interval",
            F::custom(|input| {
                Ok(ScalarUDF::from(
                    SparkMakeInterval::new()
                        .with_ansi_mode(input.function_context.plan_config.ansi_mode),
                )
                .call(input.arguments))
            }),
        ),
        ("make_time", F::udf(SparkMakeTime::new())),
        (
            "make_timestamp",
            F::custom(|input| make_timestamp(input, None, false)),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| make_timestamp(input, Some(false), false)),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| make_timestamp(input, Some(true), false)),
        ),
        ("make_ym_interval", F::udf(SparkMakeYmInterval::new())),
        ("minute", F::custom(|input| time_field(input, "MINUTE"))),
        ("month", F::custom(|input| date_field(input, "MONTH"))),
        (
            "monthname",
            F::unary(|arg| expr_fn::to_char(arg, lit("%b"))),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        ("quarter", F::custom(|input| date_field(input, "QUARTER"))),
        ("second", F::custom(|input| time_field(input, "SECOND"))),
        ("session_window", F::unknown("session_window")),
        ("time_bucket", F::unknown("time_bucket")),
        ("time_from_micros", F::unknown("time_from_micros")),
        ("time_from_millis", F::unknown("time_from_millis")),
        ("time_from_seconds", F::unknown("time_from_seconds")),
        ("time_to_micros", F::unknown("time_to_micros")),
        ("time_to_millis", F::unknown("time_to_millis")),
        ("time_to_seconds", F::unknown("time_to_seconds")),
        (
            "timestamp_micros",
            F::cast(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
        ),
        (
            "timestamp_millis",
            F::udf(SparkTimestampFromNumber::new(TimestampNumberMode::Millis)),
        ),
        (
            "timestamp_seconds",
            F::udf(SparkTimestampFromNumber::new(TimestampNumberMode::Seconds)),
        ),
        ("timestampadd", F::custom(timestampadd)),
        ("timestamp_add", F::custom(timestampadd)),
        ("timestampdiff", F::custom(datediff)),
        ("timestamp_diff", F::custom(datediff)),
        ("to_date", F::custom(to_date)),
        ("try_to_date", F::custom(try_to_date)),
        ("to_time", F::custom(to_time)),
        ("try_to_time", F::custom(try_to_time)),
        (
            "to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                to_timestamp(input, timestamp_ntz, "to_timestamp")
            }),
        ),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, false, "to_timestamp_ltz")),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, true, "to_timestamp_ntz")),
        ),
        ("to_unix_timestamp", F::custom(to_unix_timestamp)),
        (
            "to_utc_timestamp",
            F::custom(|input| utc_timestamp(input, false)),
        ),
        (
            "trunc",
            F::custom(|input| {
                let (date, part) = input.arguments.two()?;
                let date = if matches!(
                    date.get_type(input.function_context.schema)?,
                    DataType::Timestamp(_, Some(_))
                ) {
                    datetime_date(date, &input.function_context)?
                } else {
                    date
                };
                Ok(trunc(date, part))
            }),
        ),
        ("try_make_interval", F::udf(SparkMakeInterval::new())),
        (
            "try_make_timestamp",
            F::custom(|input| make_timestamp(input, None, true)),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| make_timestamp(input, Some(false), true)),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| make_timestamp(input, Some(true), true)),
        ),
        (
            "try_to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                try_to_timestamp(input, timestamp_ntz)
            }),
        ),
        ("time_diff", F::udf(SparkTimeDiff::new())),
        ("time_trunc", F::udf(SparkTimeTrunc::new())),
        (
            "unix_date",
            F::custom(|input| {
                let arg = input.arguments.one()?;
                let arg = if matches!(
                    arg.get_type(input.function_context.schema)?,
                    DataType::Timestamp(_, Some(_))
                ) {
                    datetime_date(arg, &input.function_context)?
                } else {
                    arg
                };
                Ok(cast(cast(arg, DataType::Date32), DataType::Int32))
            }),
        ),
        (
            "unix_micros",
            F::custom(|input| unix_time_unit(input, TimeUnit::Microsecond)),
        ),
        (
            "unix_millis",
            F::custom(|input| unix_time_unit(input, TimeUnit::Millisecond)),
        ),
        (
            "unix_seconds",
            F::custom(|input| unix_time_unit(input, TimeUnit::Second)),
        ),
        ("unix_timestamp", F::custom(unix_timestamp)),
        (
            "weekday",
            F::custom(|input| Ok(date_field(input, "ISODOW")? - lit(1))),
        ),
        (
            "weekofyear",
            F::unary(|arg| cast(expr_fn::to_char(arg, lit("%V")), DataType::Int32)),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        ("year", F::custom(|input| date_udf(input, SparkYear::new()))),
        ("years", F::unary(years)),
    ]
}
