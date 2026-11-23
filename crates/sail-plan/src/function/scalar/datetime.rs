// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::compute::kernels::cast_utils::IntervalUnit as DatePartUnit;
use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{
    BinaryExpr, ExprSchemable, HigherOrderUDF, Operator, ScalarUDF, cast, lit, try_cast, when,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common::utils::datetime::{MICROS_PER_DAY, is_fixed_zero_offset_timezone};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};
use sail_function::scalar::datetime::date::Date;
use sail_function::scalar::datetime::date_format::DateFormat;
use sail_function::scalar::datetime::date_part::{DatePart, IntegerDatePart};
use sail_function::scalar::datetime::date_trunc::DateTrunc;
use sail_function::scalar::datetime::last_day::LastDay;
use sail_function::scalar::datetime::make_time::MakeTime;
use sail_function::scalar::datetime::make_timestamp_ntz::MakeTimestampNtz;
use sail_function::scalar::datetime::make_ym_interval::MakeYmInterval;
use sail_function::scalar::datetime::next_day::NextDay;
use sail_function::scalar::datetime::time::Time;
use sail_function::scalar::datetime::time_diff::TimeDiff;
use sail_function::scalar::datetime::time_trunc::TimeTrunc;
use sail_function::scalar::datetime::timestamp::Timestamp;
use sail_function::scalar::datetime::timestamp_cast::{
    cast_timestamp_ltz_to_ntz, cast_timestamp_ntz_to_ltz,
};
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::datetime::unix_timestamp::UnixTimestamp;
use sail_function::scalar::datetime::window_buckets::WindowBuckets;
use sail_function::scalar::datetime::year::Year;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_python_udf::udf::expr_contains_python_udf;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use super::lambda::lambda_with_fresh_parameter;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, expr_contains_spark_cast_to_variant,
};
use crate::function::datetime_utils::{
    add_interval_in_session_timezone, has_constant_offset, with_session_timezone_label,
};

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        ScalarUDF::from(IntegerDatePart::new()).call(vec![lit(part.to_uppercase()), expr]),
        DataType::Int32,
    )
}

/// Spark implicitly casts strings, dates and wall-clock timestamps to TIMESTAMP before
/// functions such as date_trunc and months_between read them in the session time zone.
fn session_timestamp(value: Expr, context: &FunctionContextInput) -> PlanResult<Expr> {
    let zone = &context.plan_config.session_timezone;
    match value.get_type(context.schema)? {
        DataType::Timestamp(_, Some(_)) => Ok(value),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) => {
            Ok(cast_timestamp_ntz_to_ltz(value, zone, false))
        }
        ty if ty.is_string() => Ok(ScalarUDF::from(Timestamp::try_new(
            Some(Arc::clone(zone)),
            context.plan_config.ansi_mode,
            false,
        )?)
        .call(vec![value])),
        _ => Ok(cast(
            value,
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(context.plan_config.session_timezone_label()),
            ),
        )),
    }
}

/// A string implicitly cast to DATE contributes its written calendar date, even when its
/// optional time and offset would place the instant on a different day.
fn calendar_date_argument(value: Expr, context: &FunctionContextInput) -> PlanResult<Expr> {
    if value.get_type(context.schema)?.is_string() {
        Ok(ScalarUDF::from(Date::new(!context.plan_config.ansi_mode)).call(vec![value]))
    } else {
        Ok(value)
    }
}

fn date_udf(mut input: ScalarFunctionInput, udf: ScalarUDF) -> PlanResult<Expr> {
    if let Some(value) = input.arguments.first_mut() {
        *value = calendar_date_argument(value.clone(), &input.function_context)?;
    }
    Ok(udf.call(input.arguments))
}

fn date_part_argument(
    value: Expr,
    part: Option<&str>,
    context: &FunctionContextInput,
) -> PlanResult<Expr> {
    let value = if value.get_type(context.schema)?.is_string()
        && part.is_some_and(|p| {
            matches!(
                p.parse::<DatePartUnit>(),
                Ok(DatePartUnit::Hour | DatePartUnit::Minute | DatePartUnit::Second)
            )
        }) {
        session_timestamp(value, context)?
    } else if part.is_some_and(|part| !part.eq_ignore_ascii_case("epoch")) {
        calendar_date_argument(value, context)?
    } else {
        value
    };
    let timezone = &context.plan_config.session_timezone;
    let label = context.plan_config.session_timezone_label();
    // Arrow cannot label offsets with seconds. Extract their fields from the local wall clock.
    if needs_wall_clock(context)
        && matches!(
            value.get_type(context.schema)?,
            DataType::Timestamp(_, Some(_))
        )
    {
        return Ok(cast_timestamp_ltz_to_ntz(value, timezone, false));
    }
    with_session_timezone_label(value, context.schema, &label)
}

fn session_date_part(input: ScalarFunctionInput, part: &str) -> PlanResult<Expr> {
    let value = input.arguments.one()?;
    let context = &input.function_context;
    let value = date_part_argument(value, Some(part), context)?;
    Ok(integer_part(value, part))
}

fn needs_wall_clock(context: &FunctionContextInput) -> bool {
    context.plan_config.session_timezone_label().as_ref() == "UTC"
        && !is_fixed_zero_offset_timezone(&context.plan_config.session_timezone)
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

fn trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (date, part) = input.arguments.two()?;
    // Spark truncates the date of a TIMESTAMP in the session time zone.
    let date = date_part_argument(date, Some("day"), &input.function_context)?;
    Ok(cast(
        expr_fn::date_trunc(trunc_part_conversion(part), date),
        DataType::Date32,
    ))
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    // Spark truncates TIMESTAMP in the session time zone, whatever zone labels the values.
    let timestamp = with_session_timezone_label(
        session_timestamp(timestamp, &input.function_context)?,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone_label(),
    )?;
    let context = &input.function_context;
    let timestamp = if needs_wall_clock(context) {
        cast_timestamp_ltz_to_ntz(timestamp, &context.plan_config.session_timezone, false)
    } else {
        timestamp
    };
    let truncated =
        ScalarUDF::from(DateTrunc::new()).call(vec![trunc_part_conversion(part), timestamp]);
    let truncated = match truncated.get_type(input.function_context.schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, _) => truncated,
        DataType::Timestamp(_, tz) => {
            cast(truncated, DataType::Timestamp(TimeUnit::Microsecond, tz))
        }
        other => Err(PlanError::InternalError(format!(
            "date_trunc expected a timestamp result, got {other:?}"
        )))?,
    };
    Ok(if needs_wall_clock(context) {
        cast_timestamp_ntz_to_ltz(truncated, &context.plan_config.session_timezone, false)
    } else {
        truncated
    })
}

fn interval_arithmetic(input: ScalarFunctionInput, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = input.arguments.two()?;
    let date = calendar_date_argument(date, &input.function_context)?;

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
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(cast(date, DataType::Date32)),
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

fn timestampadd_interval(function_name: &str, unit: &str, quantity: Expr) -> PlanResult<Expr> {
    let zero_i32 = || lit(0_i32);
    let zero_f64 = || lit(0_f64);
    let quantity_i32 = || cast(quantity.clone(), DataType::Int32);
    let quantity_f64 = || cast(quantity.clone(), DataType::Float64);
    let make_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeInterval::new()).call(args);
    let make_dt_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeDtInterval::new()).call(args);

    let normalized = unit.trim().to_uppercase();
    match normalized.as_str() {
        "YEAR" => Ok(make_interval(vec![quantity_i32()])),
        "QUARTER" => Ok(make_interval(vec![
            zero_i32(),
            cast(quantity.clone() * lit(3_i32), DataType::Int32),
        ])),
        "MONTH" => Ok(make_interval(vec![zero_i32(), quantity_i32()])),
        "WEEK" => Ok(make_dt_interval(vec![
            cast(quantity.clone() * lit(7_i32), DataType::Int32),
            zero_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "DAY" | "DAYOFYEAR" => Ok(make_dt_interval(vec![
            quantity_i32(),
            zero_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "HOUR" => Ok(make_dt_interval(vec![
            zero_i32(),
            quantity_i32(),
            zero_i32(),
            zero_f64(),
        ])),
        "MINUTE" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            quantity_i32(),
            zero_f64(),
        ])),
        "SECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64(),
        ])),
        "MILLISECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64() / lit(1_000_f64),
        ])),
        "MICROSECOND" => Ok(make_dt_interval(vec![
            zero_i32(),
            zero_i32(),
            zero_i32(),
            quantity_f64() / lit(1_000_000_f64),
        ])),
        _ => Err(PlanError::invalid(format!(
            "{function_name} does not support interval unit type '{unit}'"
        ))),
    }
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
    let interval = timestampadd_interval(function_name, &unit, quantity)?;
    // Spark adds to a TIMESTAMP_NTZ value as a wall-clock time, and keeps its type.
    if let DataType::Timestamp(_, None) = timestamp.get_type(input.function_context.schema)? {
        return Ok(timestamp + interval);
    }
    let timezone = input.function_context.plan_config.session_timezone_label();
    let timestamp = cast(
        timestamp,
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(&timezone))),
    );
    // Spark adds days, weeks, months, quarters and years to the wall-clock time in the session
    // time zone, and shorter units to the instant (`DateTimeUtils.timestampAdd`).
    let calendar_unit = matches!(
        unit.trim().to_uppercase().as_str(),
        "DAY" | "DAYOFYEAR" | "WEEK" | "MONTH" | "QUARTER" | "YEAR"
    );
    if calendar_unit && !has_constant_offset(&timezone) {
        return Ok(add_interval_in_session_timezone(
            timestamp, interval, false, &timezone,
        ));
    }
    Ok(timestamp + interval)
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

fn make_date(year: Expr, month: Expr, day: Expr) -> Expr {
    match (&year, &month, &day) {
        (Expr::Literal(ScalarValue::Null, metadata), _, _)
        | (_, Expr::Literal(ScalarValue::Null, metadata), _)
        | (_, _, Expr::Literal(ScalarValue::Null, metadata)) => {
            Expr::Literal(ScalarValue::Null, metadata.clone())
        }
        _ => expr_fn::make_date(year, month, day),
    }
}

fn date_days_arithmetic(dt1: Expr, dt2: Expr, op: Operator) -> Expr {
    let (dt1, dt2) = match (&dt1, &dt2) {
        (Expr::Literal(ScalarValue::Date32(_), _), Expr::Literal(ScalarValue::Date32(_), _)) => {
            (dt1, dt2)
        }
        _ => (cast(dt1, DataType::Date32), cast(dt2, DataType::Date32)),
    };
    let dt1 = cast(dt1, DataType::Int64);
    let dt2 = cast(dt2, DataType::Int64);
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

/// Spark's `LocalDateTime.until(end, DAYS)`, which counts whole days and truncates toward zero.
fn timestamp_day_diff(start: Expr, end: Expr) -> Expr {
    let day = lit(86_400_000_000_i64);
    let day_number = |value: Expr| {
        let micros = timestamp_micros(value);
        micros.clone() / day.clone()
            - when((micros.clone() % day.clone()).lt(lit(0_i64)), lit(1_i64))
                .otherwise(lit(0_i64))
                .unwrap_or(lit(0_i64))
    };
    let start = cast(start, DataType::Timestamp(TimeUnit::Microsecond, None));
    let end = cast(end, DataType::Timestamp(TimeUnit::Microsecond, None));
    let days = day_number(end.clone()) - day_number(start.clone());
    let (start_time, end_time) = (timestamp_time_micros(start), timestamp_time_micros(end));
    days.clone()
        - when(
            days.clone()
                .gt(lit(0_i64))
                .and(end_time.clone().lt(start_time.clone())),
            lit(1_i64),
        )
        .when(
            days.lt(lit(0_i64)).and(end_time.gt(start_time)),
            lit(-1_i64),
        )
        .otherwise(lit(0_i64))
        .unwrap_or(lit(0_i64))
}

/// Microseconds since midnight. `date_trunc` works in nanoseconds, which fail outside 1677-2262.
fn timestamp_time_micros(expr: Expr) -> Expr {
    let day = lit(86_400_000_000_i64);
    ((timestamp_micros(expr) % day.clone()) + day.clone()) % day
}

fn timestamp_months(expr: Expr) -> Expr {
    integer_part(expr.clone(), "YEAR") * lit(12) + integer_part(expr, "MONTH")
}

/// Spark's `LocalDateTime.until(end, MONTHS)`, which moves the end date a day toward the start when
/// its time of day is not reached, and then counts whole months by the day of month.
fn timestamp_month_diff(start: Expr, end: Expr) -> PlanResult<Expr> {
    let start = cast(start, DataType::Timestamp(TimeUnit::Microsecond, None));
    let end = cast(end, DataType::Timestamp(TimeUnit::Microsecond, None));
    let start_date = cast(start.clone(), DataType::Date32);
    let end_date = cast(end.clone(), DataType::Date32);
    let start_time = timestamp_time_micros(start);
    let end_time = timestamp_time_micros(end);
    let end_day = cast(end_date.clone(), DataType::Int32);
    let end_date = cast(
        when(
            end_date
                .clone()
                .gt(start_date.clone())
                .and(end_time.clone().lt(start_time.clone())),
            end_day.clone() - lit(1_i32),
        )
        .when(
            end_date.lt(start_date.clone()).and(end_time.gt(start_time)),
            end_day.clone() + lit(1_i32),
        )
        .otherwise(end_day)?,
        DataType::Date32,
    );
    let packed = |date: Expr| {
        cast(timestamp_months(date.clone()), DataType::Int64) * lit(32_i64)
            + cast(integer_part(date, "DAY"), DataType::Int64)
    };
    Ok((packed(end_date) - packed(start_date)) / lit(32_i64))
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
    let start_ts = timestamp_micros(start);
    let end_ts = timestamp_micros(end);
    let diff_micros = cast(
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(end_ts),
            op: Operator::Minus,
            right: Box::new(start_ts),
        }),
        DataType::Int64,
    );
    let divisor = match unit {
        "SECOND" => 1_000_000i64,
        "MINUTE" => 60_000_000i64,
        "HOUR" => 3_600_000_000i64,
        "WEEK" => 7 * 24 * 3_600_000_000i64,
        _ => 1i64,
    };
    diff_micros / lit(divisor)
}

fn datediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments: args,
        function_context,
    } = input;
    match args.len() {
        2 => {
            let [start, end] = <[Expr; 2]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            Ok(date_days_arithmetic(
                calendar_date_argument(start, &function_context)?,
                calendar_date_argument(end, &function_context)?,
                Operator::Minus,
            ))
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
            match unit_str.as_str() {
                // Spark counts every unit between the wall-clock times in the session time zone.
                "DAY" | "MONTH" | "YEAR" | "QUARTER" | "HOUR" | "MINUTE" | "SECOND" | "WEEK" => {
                    // The session time zone as configured, which `ConvertTz` reads like Spark. The
                    // label would read an offset with seconds as UTC.
                    let session_timezone = &function_context.plan_config.session_timezone;
                    let timezone = session_timezone.as_ref();
                    let is_fixed_offset =
                        timezone.starts_with(['+', '-']) || is_fixed_zero_offset_timezone(timezone);
                    let is_ltz = |value: &Expr| {
                        matches!(
                            value.get_type(function_context.schema),
                            Ok(DataType::Timestamp(_, Some(_)))
                        )
                    };
                    let has_ltz = is_ltz(&start) || is_ltz(&end);
                    let local = |value: Expr| -> PlanResult<Expr> {
                        Ok(match value.get_type(function_context.schema)? {
                            DataType::Timestamp(_, Some(_)) => {
                                cast_timestamp_ltz_to_ntz(value, session_timezone, false)
                            }
                            // Spark casts TIMESTAMP_NTZ to TIMESTAMP when the other value is one,
                            // which moves a wall-clock time in a DST gap forward, and fails with
                            // long overflow near the ends of the range, also at a fixed offset.
                            DataType::Timestamp(_, None)
                                if has_ltz && !is_fixed_zero_offset_timezone(timezone) =>
                            {
                                cast_timestamp_ltz_to_ntz(
                                    cast_timestamp_ntz_to_ltz(value, session_timezone, false),
                                    session_timezone,
                                    false,
                                )
                            }
                            _ => value,
                        })
                    };
                    // A fixed offset moves both wall-clock times of TIMESTAMP values alike.
                    let same_offset = is_fixed_offset && is_ltz(&start) && is_ltz(&end);
                    match unit_str.as_str() {
                        "DAY" => Ok(timestamp_day_diff(local(start)?, local(end)?)),
                        "HOUR" | "MINUTE" | "SECOND" | "WEEK" if same_offset => {
                            Ok(timestampdiff_fixed_unit(&unit_str, start, end))
                        }
                        "HOUR" | "MINUTE" | "SECOND" | "WEEK" => Ok(timestampdiff_fixed_unit(
                            &unit_str,
                            local(start)?,
                            local(end)?,
                        )),
                        _ => timestampdiff_calendar_unit(&unit_str, local(start)?, local(end)?),
                    }
                }
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

/// Formats the date of a value. `to_char` formats TIMESTAMP values in nanoseconds, which fail
/// before 1677, so they are cast to the date in the time zone of their label, which `to_char`
/// would read.
fn date_to_char(input: ScalarFunctionInput, format: &str) -> PlanResult<Expr> {
    let value = calendar_date_argument(input.arguments.one()?, &input.function_context)?;
    let value = match value.get_type(input.function_context.schema)? {
        DataType::Timestamp(_, Some(_)) => cast(value, DataType::Date32),
        _ => value,
    };
    Ok(expr_fn::to_char(value, lit(format)))
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
            Ok(ScalarUDF::from(Date::new(null_on_error)).call(vec![expr]))
        } else {
            Ok(cast(expr, DataType::Date32))
        }
    } else if input.arguments.len() == 2 {
        let expr = input.arguments[0].clone();
        let format = coerce_datetime_format(
            "to_date",
            input.arguments[1].clone(),
            input.function_context.schema,
        )?;
        let expr_type = expr.get_type(input.function_context.schema);
        if matches!(
            expr_type,
            Ok(DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _))
        ) {
            let nullable = !input.function_context.plan_config.ansi_mode
                || expr.nullable(input.function_context.schema)?
                || format.nullable(input.function_context.schema)?;
            // GetTimestamp uses the stored microseconds for a timestamp argument, including NTZ.
            let expr = if matches!(expr_type, Ok(DataType::Timestamp(_, None))) {
                cast(
                    timestamp_micros(expr),
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some(input.function_context.plan_config.session_timezone_label()),
                    ),
                )
            } else {
                expr
            };
            return declare_nullable_result(
                cast(expr, DataType::Date32),
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
        Ok(ScalarUDF::from(Date::new(null_on_error)).call(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_date requires 1 or 2 arguments"))
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
        Ok(ScalarUDF::from(UnixTimestamp::new(timezone, ansi_mode)).call(input.arguments))
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
            let result = ScalarUDF::from(UnixTimestamp::new(timezone, ansi_mode)).call(vec![expr]);
            declare_nullable_result(result, nullable, input.function_context.schema)
        } else {
            Ok(ScalarUDF::from(UnixTimestamp::new(timezone, ansi_mode)).call(vec![expr, format]))
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
    let udf = ScalarUDF::from(NextDay::new(ansi_mode));
    date_udf(input, udf)
}

pub(super) fn date_format(
    value: Expr,
    format: Expr,
    context: &FunctionContextInput,
) -> PlanResult<Expr> {
    let timezone = &context.plan_config.session_timezone;
    let value = if matches!(
        value.get_type(context.schema)?,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) {
        session_timestamp(value, context)?
    } else {
        value
    };
    Ok(ScalarUDF::from(DateFormat::new(Arc::clone(timezone))).call(vec![value, format]))
}

fn timestamp_data_type(input: &ScalarFunctionInput, timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone_label())
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn timestamp_null(input: &ScalarFunctionInput, timestamp_ntz: bool) -> Expr {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone_label())
    };
    lit(ScalarValue::TimestampMicrosecond(None, timezone))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

pub(super) fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, false)
}

fn to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, false)
}

fn try_to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, true)
}

/// Shared `to_time` / `try_to_time` planner. Routes through `Time`, which
/// parses strings (with an optional Spark Java datetime pattern) or casts
/// time/timestamp args.
/// `to_time` errors on failure (Spark's `ToTime` is ANSI-invariant); `try_to_time`
/// (`is_try`) returns NULL.
fn time_with_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let udf = ScalarUDF::from(Time::new(is_try));
    if input.arguments.len() == 1 {
        Ok(udf.call(input.arguments))
    } else if input.arguments.len() == 2 {
        // Pass `expr` through unchanged so `Time::coerce_types` validates it
        // and the kernel dispatches by type (strings parse with the format,
        // TIME/TIMESTAMP cast directly), exactly as in the 1-arg form. Forcing a
        // cast to Utf8 here would route non-string inputs through string parsing,
        // bypassing the coercion checks and diverging from the 1-arg behavior.
        let (expr, format) = input.arguments.two()?;
        Ok(udf.call(vec![expr, format]))
    } else {
        let name = if is_try { "try_to_time" } else { "to_time" };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
    }
}

fn try_to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, true)
}

/// Shared `to_timestamp` / `try_to_timestamp` (+ `_ntz`) planner.
///
/// The 1-arg form goes through `cast` / `try_cast`, which route strings to
/// `Timestamp` (honoring ANSI for the strict variant) and cast other types.
/// The 2-arg form parses the value with the given format via `Timestamp`.
fn timestamp_with_try(
    input: ScalarFunctionInput,
    timestamp_ntz: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        let expr_type = expr.get_type(input.function_context.schema)?;
        if matches!(
            expr_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            // Strings parse through Timestamp, which honors ANSI (errors
            // under ANSI, NULL otherwise) for the strict variant.
            let udf = ScalarUDF::from(Timestamp::try_new(timezone, ansi_mode, is_try)?);
            Ok(udf.call(vec![expr]))
        } else {
            match (&expr_type, &data_type) {
                (DataType::Timestamp(_, Some(_)), DataType::Timestamp(_, None)) => {
                    return Ok(cast_timestamp_ltz_to_ntz(
                        expr,
                        &input.function_context.plan_config.session_timezone,
                        is_try,
                    ));
                }
                (
                    DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64,
                    DataType::Timestamp(_, Some(_)),
                ) => {
                    return Ok(cast_timestamp_ntz_to_ltz(
                        expr,
                        &input.function_context.plan_config.session_timezone,
                        is_try,
                    ));
                }
                _ => {}
            }
            if is_try {
                Ok(try_cast(expr, data_type))
            } else {
                Ok(cast(expr, data_type))
            }
        }
    } else if input.arguments.len() == 2 {
        let value = &input.arguments[0];
        let schema = input.function_context.schema;
        let value_type = value.get_type(schema)?;
        if matches!(
            value_type,
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
        ) {
            // Spark's GetTimestamp takes the stored microseconds for either timestamp type and
            // interprets a date at local midnight. Only string inputs evaluate the format.
            let format =
                coerce_datetime_format("to_timestamp", input.arguments[1].clone(), schema)?;
            let nullable =
                is_try || !ansi_mode || value.nullable(schema)? || format.nullable(schema)?;
            let value = match value_type {
                DataType::Timestamp(_, _) => value.clone(),
                _ => cast_timestamp_ntz_to_ltz(
                    value.clone(),
                    &input.function_context.plan_config.session_timezone,
                    is_try,
                ),
            };
            let micros = timestamp_micros(value);
            return declare_nullable_result(cast(micros, data_type), nullable, schema);
        }
        let null = timestamp_null(&input, timestamp_ntz);
        if input.arguments.iter().any(is_null_literal) {
            return Ok(null);
        }
        let arguments = input.arguments;
        Ok(cast(
            ScalarUDF::from(Timestamp::try_new(timezone, ansi_mode, is_try)?).call(arguments),
            data_type,
        ))
    } else {
        let name = match (is_try, timestamp_ntz) {
            (false, false) => "to_timestamp",
            (true, false) => "try_to_timestamp",
            (false, true) => "to_timestamp_ntz",
            (true, true) => "try_to_timestamp_ntz",
        };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
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
        return Err(PlanError::invalid(
            "from_unixtime requires 1 or 2 arguments",
        ));
    }?;

    let timezone = input.function_context.plan_config.session_timezone.clone();
    let expr = cast(
        expr,
        DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
    );
    date_format(expr, format, &input.function_context)
}

fn unix_time_unit(input: ScalarFunctionInput, time_unit: TimeUnit) -> PlanResult<Expr> {
    let arg = input.arguments.one()?;
    Ok(cast(
        cast(
            arg,
            DataType::Timestamp(
                time_unit,
                Some(input.function_context.plan_config.session_timezone.clone()),
            ),
        ),
        DataType::Int64,
    ))
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
    Ok(expr_fn::to_local_time(vec![expr]))
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

// TODO: Reject every non-atomic time-zone type during analysis, even when Arrow can cast
//  it to Utf8 (for example, ARRAY). Spark's VariantType remains a supported atomic type.
fn validate_convert_timezone_zone(expr: &Expr, schema: &DFSchemaRef) -> PlanResult<()> {
    let field = expr.to_field(schema)?.1;
    if !can_cast_types(field.data_type(), &DataType::Utf8)
        && !is_variant_storage_field(field.as_ref())
    {
        return Err(PlanError::invalid(format!(
            "convert_timezone time zone arguments must be castable to string, got {}",
            field.data_type()
        )));
    }
    Ok(())
}

/// A helper function for processing the input NTZ timestamp.
fn ntz_timestamp_and_unit(
    ts: Expr,
    timezone: &str,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    match ts.get_type(schema)? {
        DataType::Timestamp(_, Some(_)) => Ok((
            cast_timestamp_ltz_to_ntz(ts, timezone, false),
            TimeUnit::Microsecond,
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
            let ts = ScalarUDF::from(Timestamp::try_new(None, ansi_mode, false)?).call(vec![ts]);
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
    let (ts, _unit) = ntz_timestamp_and_unit(
        ts,
        &session_tz,
        input.function_context.schema,
        input.function_context.plan_config.ansi_mode,
    )?;
    let requires_scalar_evaluation =
        [&from_tz, &to_tz, &ts]
            .into_iter()
            .try_fold(false, |found, argument| {
                if found {
                    PlanResult::Ok(true)
                } else {
                    Ok(expr_contains_python_udf(argument)?
                        || expr_contains_spark_cast_to_variant(argument)?)
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
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts = ScalarUDF::from(Timestamp::try_new(
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

// TODO: Match Spark's foldability-aware zone eval for `from_utc_timestamp` and `to_utc_timestamp`:
//  a foldable NULL skips the timestamp, a non-NULL foldable zone is validated eagerly,
//  and a dynamic zone is skipped for a NULL timestamp.
fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, to_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(lit("UTC"), to_tz, ts, false, false)?;
    // The wall-clock time in UTC is the instant. Arrow's cast would compute it with Chrono, which
    // does not represent Spark's full range.
    let ts = cast(
        cast(ts, DataType::Int64),
        DataType::Timestamp(unit, Some(Arc::from("UTC"))),
    );
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, from_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(from_tz, lit("UTC"), ts, false, false)?;
    // The wall-clock time in UTC is the instant. Arrow's cast would compute it with Chrono, which
    // does not represent Spark's full range.
    let ts = cast(
        cast(ts, DataType::Int64),
        DataType::Timestamp(unit, Some(Arc::from("UTC"))),
    );
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn make_timestamp_ltz(args: Vec<Expr>, session_tz: &Arc<str>, is_try: bool) -> PlanResult<Expr> {
    let ntz_ts = if args.len() == 2 || args.len() == 6 {
        ScalarUDF::from(MakeTimestampNtz::new(is_try)).call(args)
    } else if args.len() == 3 || args.len() == 7 {
        let mut args = args;
        let Some(from_tz) = args.pop() else {
            unreachable!()
        };
        let ntz_ts = ScalarUDF::from(MakeTimestampNtz::new(is_try)).call(args);
        convert_tz(from_tz, lit(session_tz.to_string()), ntz_ts, true, false)?
    } else {
        return Err(PlanError::invalid(format!(
            "{}make_timestamp_ltz requires 2, 3, 6 or 7 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )));
    };
    Ok(cast(
        ntz_ts,
        DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone())),
    ))
}

fn make_timestamp_ntz(args: Vec<Expr>, is_try: bool) -> PlanResult<Expr> {
    if args.len() == 2 || args.len() == 6 {
        Ok(ScalarUDF::from(MakeTimestampNtz::new(is_try)).call(args))
    } else {
        Err(PlanError::invalid(format!(
            "{}make_timestamp_ntz requires 2 or 6 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )))
    }
}

fn make_timestamp(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let session_tz = &input.function_context.plan_config.session_timezone;
    let mut args = input.arguments;
    if args.len() == 1 {
        args.push(lit(ScalarValue::Time64Microsecond(Some(0))));
    }
    match input.function_context.plan_config.default_timestamp_type {
        DefaultTimestampType::TimestampLtz => make_timestamp_ltz(args, session_tz, is_try),
        DefaultTimestampType::TimestampNtz => {
            if args.len() == 3 || args.len() == 7 {
                args.pop();
            }
            make_timestamp_ntz(args, is_try)
        }
    }
}

fn date_part(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, date) = input.arguments.two()?;
    let field = match &part {
        Expr::Literal(value, _) => value.try_as_str().flatten(),
        _ => None,
    };
    let date = date_part_argument(date, field, &input.function_context)?;
    Ok(ScalarUDF::from(DatePart::new()).call(vec![part, date]))
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

    // Spark counts seconds since local midnight, including days with a DST transition.
    let session_timezone = &function_context.plan_config.session_timezone;
    let local_date_and_seconds = |dt: Expr| -> PlanResult<(Expr, Expr)> {
        let instant = session_timestamp(dt, &function_context)?;
        let local = cast_timestamp_ltz_to_ntz(instant.clone(), session_timezone, false);
        let day_start = cast_timestamp_ntz_to_ltz(
            cast(
                timestamp_micros(local.clone()) - timestamp_time_micros(local.clone()),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            session_timezone,
            false,
        );
        let seconds =
            (timestamp_micros(instant) - timestamp_micros(day_start)) / lit(1_000_000_i64);
        Ok((local, seconds))
    };

    let date_to_months =
        |dt: Expr| integer_part(dt.clone(), "YEAR") * lit(12) + integer_part(dt, "MONTH");

    let is_last_day = |dt: Expr| {
        ScalarUDF::from(LastDay::new())
            .call(vec![cast(dt.clone(), DataType::Date32)])
            .eq(cast(dt, DataType::Date32))
    };

    // prerequisites
    let (date1, seconds1) = local_date_and_seconds(date1)?;
    let (date2, seconds2) = local_date_and_seconds(date2)?;

    // calculations:
    let days1 = integer_part(date1.clone(), "DAY");
    let days2 = integer_part(date2.clone(), "DAY");

    let month_diff = cast(
        date_to_months(date1.clone()) - date_to_months(date2.clone()),
        DataType::Float64,
    );

    let seconds_diff = (days1.clone() - days2.clone()) * lit(seconds_per_day) + seconds1 - seconds2;

    let months_between = when(
        days1
            .eq(days2)
            .or(is_last_day(date1).and(is_last_day(date2))),
        month_diff.clone(),
    )
    .when(lit(true), month_diff + seconds_diff / seconds_in_month)
    .end()?;

    Ok(when(
        round_off,
        expr_fn::round(vec![months_between.clone(), lit(8)]),
    )
    .when(lit(true), months_between)
    .end()?)
}

/// Parses a `window` duration/start-time argument (interval string, day-time
/// interval, or integer number of microseconds, matching Spark's
/// `TimeWindow.parseExpression`) into microseconds. Months/years are rejected
/// (non-constant length).
fn window_interval_micros(expr: &Expr) -> PlanResult<i64> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlanError::invalid(
            "window durations and start time must be literal strings, intervals, or integers",
        ));
    };
    if let Some(s) = value.try_as_str().flatten() {
        return match parse_interval(s)
            .map_err(|e| PlanError::invalid(format!("invalid window interval {s:?}: {e}")))?
        {
            IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
            _ => Err(PlanError::invalid(format!(
                "window interval must not contain months or years: {s:?}"
            ))),
        };
    }
    match value {
        // Spark interprets integer literals as microseconds.
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::DurationMicrosecond(Some(v)) => Ok(*v),
        ScalarValue::DurationMillisecond(Some(v)) => Ok(*v * 1_000),
        ScalarValue::DurationSecond(Some(v)) => Ok(*v * 1_000_000),
        ScalarValue::DurationNanosecond(Some(v)) => Ok(*v / 1_000),
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.milliseconds as i64 * 1_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.months == 0 => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.nanoseconds / 1_000)
        }
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

/// Parses and validates the `window` durations from the full argument list
/// (`args[0]` is the time column; `args[1..]` are window/slide/start).
fn parse_window_spec(args: &[Expr]) -> PlanResult<WindowSpec> {
    if !(2..=4).contains(&args.len()) {
        return Err(PlanError::invalid(format!(
            "window requires 2 to 4 arguments, got {}",
            args.len()
        )));
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
        return Err(PlanError::invalid(
            "window: the window duration must be greater than 0",
        ));
    }
    if slide_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the slide duration must be greater than 0",
        ));
    }
    if slide_duration > window_duration {
        return Err(PlanError::invalid(
            "window: the slide duration must be less than or equal to the window duration",
        ));
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
            return Err(PlanError::invalid(format!(
                "window requires a timestamp time column, got {other:?}"
            )));
        }
    })
}

/// The Spark `window` time function: buckets a timestamp into `struct<start, end>`
/// windows. The candidate enumeration is deferred to the `WindowBuckets` UDF
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
    let field_type = window_field_type(&time.get_type(schema)?, &session_tz)?;
    let time_ts = cast(time, field_type);
    let buckets = ScalarUDF::from(WindowBuckets::new(
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
        other => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {other:?}"
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
        ("curdate", F::nullary(expr_fn::current_date)),
        ("current_date", F::nullary(expr_fn::current_date)),
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
            F::custom(|input| {
                let (value, format) = input.arguments.two()?;
                date_format(value, format, &input.function_context)
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
        ("day", F::custom(|input| session_date_part(input, "DAY"))),
        ("dayname", F::custom(|input| date_to_char(input, "%a"))),
        (
            "dayofmonth",
            F::custom(|input| session_date_part(input, "DAY")),
        ),
        (
            "dayofweek",
            F::custom(|input| Ok(session_date_part(input, "DOW")? + lit(1))),
        ),
        (
            "dayofyear",
            F::custom(|input| session_date_part(input, "DOY")),
        ),
        ("extract", F::custom(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::custom(from_utc_timestamp)),
        ("hour", F::custom(|input| session_date_part(input, "HOUR"))),
        (
            "last_day",
            F::custom(|input| date_udf(input, ScalarUDF::from(LastDay::new()))),
        ),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::ternary(make_date)),
        ("make_dt_interval", F::udf(SparkMakeDtInterval::new())),
        ("make_interval", F::udf(SparkMakeInterval::new())),
        ("make_time", F::udf(MakeTime::new())),
        (
            "make_timestamp",
            F::custom(|input| make_timestamp(input, false)),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    false,
                )
            }),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, false)),
        ),
        ("make_ym_interval", F::udf(MakeYmInterval::new())),
        (
            "minute",
            F::custom(|input| session_date_part(input, "MINUTE")),
        ),
        (
            "month",
            F::custom(|input| session_date_part(input, "MONTH")),
        ),
        ("monthname", F::custom(|input| date_to_char(input, "%b"))),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        (
            "quarter",
            F::custom(|input| session_date_part(input, "QUARTER")),
        ),
        (
            "second",
            F::custom(|input| session_date_part(input, "SECOND")),
        ),
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
            F::unary(|arg| {
                cast(
                    cast(arg, DataType::Int64) * lit(1_000_i64),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )
            }),
        ),
        (
            "timestamp_seconds",
            F::unary(|arg| {
                cast(
                    cast(arg, DataType::Int64) * lit(1_000_000_i64),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )
            }),
        ),
        ("timestampadd", F::custom(timestampadd)),
        ("timestamp_add", F::custom(timestampadd)),
        ("timestampdiff", F::custom(datediff)),
        ("timestamp_diff", F::custom(datediff)),
        ("to_date", F::custom(to_date)),
        ("to_time", F::custom(to_time)),
        ("try_to_time", F::custom(try_to_time)),
        (
            "to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                to_timestamp(input, timestamp_ntz)
            }),
        ),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, false)),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, true)),
        ),
        ("to_unix_timestamp", F::custom(to_unix_timestamp)),
        ("to_utc_timestamp", F::custom(to_utc_timestamp)),
        ("trunc", F::custom(trunc)),
        ("try_make_interval", F::udf(SparkMakeInterval::new())),
        (
            "try_make_timestamp",
            F::custom(|input| make_timestamp(input, true)),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    true,
                )
            }),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, true)),
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
        ("time_diff", F::udf(TimeDiff::new())),
        ("time_trunc", F::udf(TimeTrunc::new())),
        (
            "unix_date",
            F::unary(|arg| cast(cast(arg, DataType::Date32), DataType::Int32)),
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
            F::custom(|input| Ok((session_date_part(input, "DOW")? + lit(6)) % lit(7))),
        ),
        (
            "weekofyear",
            F::custom(|input| Ok(cast(date_to_char(input, "%V")?, DataType::Int32))),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        (
            "year",
            F::custom(|input| date_udf(input, ScalarUDF::from(Year::new()))),
        ),
        ("years", F::custom(|input| session_date_part(input, "YEAR"))),
    ]
}
