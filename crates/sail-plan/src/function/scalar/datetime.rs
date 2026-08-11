use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, Expr, FieldMetadata};
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, ScalarUDF, cast, lit, when};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_timestamp_interval::SparkTimestampInterval;
use sail_function::scalar::datetime::spark_timezone_cast::SparkTimezoneCast;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

pub(crate) fn timezone_cast(
    expression: Expr,
    target_type: DataType,
    session_timezone: &Arc<str>,
    safe: bool,
) -> Expr {
    ScalarUDF::from(SparkTimezoneCast::new(
        target_type,
        Arc::clone(session_timezone),
        safe,
    ))
    .call(vec![expression])
}

fn session_local_timestamp_if_ltz(
    expression: Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
) -> PlanResult<Expr> {
    Ok(match expression.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => timezone_cast(
            expression,
            DataType::Timestamp(unit, None),
            session_timezone,
            false,
        ),
        _ => expression,
    })
}

fn coerce_to_ltz(
    expression: Expr,
    unit: TimeUnit,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let expression = match expression.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ScalarUDF::from(
            SparkTimestamp::try_new(Some(session_timezone.clone()), ansi_mode, is_try)?,
        )
        .call(vec![expression]),
        _ => expression,
    };
    Ok(timezone_cast(
        expression,
        DataType::Timestamp(unit, Some(Arc::from("UTC"))),
        session_timezone,
        is_try,
    ))
}

fn coerce_string_argument(
    expression: Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
) -> PlanResult<Expr> {
    Ok(match expression.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => expression,
        DataType::Timestamp(_, Some(_)) => {
            ScalarUDF::from(SparkToUtf8::new(Arc::clone(session_timezone))).call(vec![expression])
        }
        _ => cast(expression, DataType::Utf8),
    })
}

fn session_local_unary(
    input: ScalarFunctionInput,
    function: impl FnOnce(Expr) -> Expr,
) -> PlanResult<Expr> {
    let argument = input.arguments.one()?;
    let argument = session_local_timestamp_if_ltz(
        argument,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    Ok(function(argument))
}

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        expr_fn::date_part(lit(part.to_uppercase()), expr),
        DataType::Int32,
    )
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

fn trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (date, part) = input.arguments.two()?;
    let part = coerce_string_argument(
        part,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    let date = session_local_timestamp_if_ltz(
        date,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    Ok(cast(
        expr_fn::date_trunc(trunc_part_conversion(part), date),
        DataType::Date32,
    ))
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    let part = coerce_string_argument(
        part,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    let timestamp = coerce_to_ltz(
        timestamp,
        TimeUnit::Microsecond,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
        input.function_context.plan_config.ansi_mode,
        false,
    )?;
    let truncated = ScalarUDF::from(SparkDateTrunc::new(
        input.function_context.plan_config.session_timezone.clone(),
    ))
    .call(vec![trunc_part_conversion(part), timestamp]);
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
    let date = session_local_timestamp_if_ltz(
        date,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;

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

fn timestampadd(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (unit, quantity, timestamp) = input.arguments.three()?;
    let unit = match &unit {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
        Expr::Column(col) => col.name().to_string(),
        _ => {
            return Err(PlanError::invalid(
                "timestampadd unit must be a string literal or keyword",
            ));
        }
    };
    let session_timezone = input.function_context.plan_config.session_timezone.clone();
    let timestamp = match timestamp.get_type(input.function_context.schema)? {
        DataType::Timestamp(_, None) => {
            cast(timestamp, DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        DataType::Timestamp(_, Some(_)) => coerce_to_ltz(
            timestamp,
            TimeUnit::Microsecond,
            input.function_context.schema,
            &session_timezone,
            input.function_context.plan_config.ansi_mode,
            false,
        )?,
        _ => coerce_to_ltz(
            timestamp,
            TimeUnit::Microsecond,
            input.function_context.schema,
            &session_timezone,
            input.function_context.plan_config.ansi_mode,
            false,
        )?,
    };
    Ok(ScalarUDF::from(SparkTimestampInterval::new_timestampadd(
        session_timezone,
        Arc::from(unit),
    ))
    .call(vec![timestamp, quantity]))
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

fn timestamp_time_micros(expr: Expr) -> Expr {
    timestamp_micros(expr.clone()) - timestamp_micros(expr_fn::date_trunc(lit("DAY"), expr))
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
        .when(lit(true), months)
        .end()?)
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
    let schema = input.function_context.schema;
    let session_timezone = &input.function_context.plan_config.session_timezone;
    let args = input.arguments;
    match args.len() {
        2 => {
            let [start, end] = <[Expr; 2]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
            let start = session_local_timestamp_if_ltz(start, schema, session_timezone)?;
            let end = session_local_timestamp_if_ltz(end, schema, session_timezone)?;
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
            let start = session_local_timestamp_if_ltz(start, schema, session_timezone)?;
            let end = session_local_timestamp_if_ltz(end, schema, session_timezone)?;
            match unit_str.as_str() {
                "DAY" => Ok(date_days_arithmetic(end, start, Operator::Minus)),
                "HOUR" | "MINUTE" | "SECOND" | "WEEK" => {
                    Ok(timestampdiff_fixed_unit(&unit_str, start, end))
                }
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
    session_timezone: &Arc<str>,
) -> PlanResult<Expr> {
    let data_type = format.get_type(schema)?;
    if data_type.is_nested() {
        Err(PlanError::invalid(format!(
            "{function_name} format argument must be a string, got {data_type}"
        )))
    } else {
        coerce_string_argument(format, schema, session_timezone)
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
    if input.arguments.len() == 1 {
        // If format is not supplied, the function is a synonym for cast(expr AS DATE).
        crate::function::scalar::conversion::cast_to_date(input)
    } else if input.arguments.len() == 2 {
        let expr = input.arguments[0].clone();
        let format = coerce_datetime_format(
            "to_date",
            input.arguments[1].clone(),
            input.function_context.schema,
            &input.function_context.plan_config.session_timezone,
        )?;
        let expr_type = expr.get_type(input.function_context.schema);
        let date = match &expr_type {
            Ok(DataType::Timestamp(_, Some(_))) => Some(session_local_timestamp_if_ltz(
                expr.clone(),
                input.function_context.schema,
                &input.function_context.plan_config.session_timezone,
            )?),
            Ok(DataType::Timestamp(_, None)) => Some(expr.clone()),
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
        let expr = match expr_type {
            Ok(_other) => expr,
            Err(_) => cast(expr, DataType::Utf8), // In case of error, cast to string
        };
        Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![expr, format]))
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
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode)).call(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = coerce_string_argument(
            format,
            input.function_context.schema,
            &input.function_context.plan_config.session_timezone,
        )?;
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
    let (date, day_of_week) = input.arguments.two()?;
    let day_of_week = coerce_string_argument(
        day_of_week,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    let date = session_local_timestamp_if_ltz(
        date,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    Ok(udf.call(vec![date, day_of_week]))
}

pub(super) fn date_format(expr: Expr, format: Expr, timezone: String) -> Expr {
    date_format_with_args(vec![expr, format], timezone)
}

fn date_format_with_args(arguments: Vec<Expr>, timezone: String) -> Expr {
    let mut arguments = arguments;
    if arguments.len() > 1 {
        arguments[1] = ScalarUDF::from(SparkToUtf8::new(Arc::from(timezone.clone())))
            .call(vec![arguments[1].clone()]);
    }
    ScalarUDF::from(SparkDateFormat::new(timezone.into())).call(arguments)
}

fn timestamp_data_type(timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(Arc::from("UTC"))
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn timestamp_null(timestamp_ntz: bool) -> Expr {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(Arc::from("UTC"))
    };
    lit(ScalarValue::TimestampMicrosecond(None, timezone))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, false)
}

fn to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, false)
}

fn try_to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, true)
}

/// Shared `to_time` / `try_to_time` planner. Routes through `SparkTime`, which
/// parses strings (with an optional Spark Java datetime pattern) or casts
/// time/timestamp args.
/// `to_time` errors on failure (Spark's `ToTime` is ANSI-invariant); `try_to_time`
/// (`is_try`) returns NULL.
fn time_with_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let udf = ScalarUDF::from(SparkTime::new(is_try));
    let mut arguments = input.arguments;
    if let Some(expression) = arguments.first_mut() {
        *expression = session_local_timestamp_if_ltz(
            expression.clone(),
            input.function_context.schema,
            &input.function_context.plan_config.session_timezone,
        )?;
    }
    if arguments.len() == 1 {
        Ok(udf.call(arguments))
    } else if arguments.len() == 2 {
        // Pass `expr` through unchanged so `SparkTime::coerce_types` validates it
        // and the kernel dispatches by type (strings parse with the format,
        // TIME/TIMESTAMP cast directly), exactly as in the 1-arg form. Forcing a
        // cast to Utf8 here would route non-string inputs through string parsing,
        // bypassing the coercion checks and diverging from the 1-arg behavior.
        let (expr, format) = arguments.two()?;
        let format = coerce_string_argument(
            format,
            input.function_context.schema,
            &input.function_context.plan_config.session_timezone,
        )?;
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
/// `SparkTimestamp` (honoring ANSI for the strict variant) and cast other types.
/// The 2-arg form parses the value with the given format via `SparkTimestamp`.
fn timestamp_with_try(
    input: ScalarFunctionInput,
    timestamp_ntz: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(timestamp_ntz);
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
            // Strings parse through SparkTimestamp, which honors ANSI (errors
            // under ANSI, NULL otherwise) for the strict variant.
            let udf = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, is_try)?);
            Ok(udf.call(vec![expr]))
        } else {
            Ok(timezone_cast(
                expr,
                data_type,
                &input.function_context.plan_config.session_timezone,
                is_try,
            ))
        }
    } else if input.arguments.len() == 2 {
        let null = timestamp_null(timestamp_ntz);
        if input.arguments.iter().any(is_null_literal) {
            return Ok(null);
        }
        let mut arguments = input.arguments;
        arguments[1] = coerce_string_argument(
            arguments[1].clone(),
            input.function_context.schema,
            &input.function_context.plan_config.session_timezone,
        )?;
        Ok(timezone_cast(
            ScalarUDF::from(SparkTimestamp::try_new(
                match &data_type {
                    DataType::Timestamp(_, Some(_)) => {
                        Some(input.function_context.plan_config.session_timezone.clone())
                    }
                    DataType::Timestamp(_, None) => None,
                    _ => None,
                },
                ansi_mode,
                is_try,
            )?)
            .call(arguments),
            data_type,
            &input.function_context.plan_config.session_timezone,
            is_try,
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
        DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
    );
    Ok(date_format(expr, format, timezone.to_string()))
}

fn unix_time_unit(input: ScalarFunctionInput, time_unit: TimeUnit) -> PlanResult<Expr> {
    let arg = input.arguments.one()?;
    let timestamp = coerce_to_ltz(
        arg,
        time_unit,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
        input.function_context.plan_config.ansi_mode,
        false,
    )?;
    Ok(cast(timestamp, DataType::Int64))
}

pub(crate) fn current_timestamp(session_timezone: &Arc<str>) -> Expr {
    ScalarUDF::from(TimestampNow::new(
        Arc::clone(session_timezone),
        TimeUnit::Microsecond,
    ))
    .call(vec![])
}

pub(crate) fn to_session_local_timestamp(timestamp: Expr, session_timezone: &Arc<str>) -> Expr {
    let timestamp = cast(timestamp, DataType::Timestamp(TimeUnit::Microsecond, None));
    convert_tz(
        lit("UTC"),
        lit(session_timezone.to_string()),
        timestamp,
        false,
    )
}

fn current_timestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
        Ok(current_timestamp(
            &input.function_context.plan_config.session_timezone,
        ))
    } else {
        Err(PlanError::invalid(format!(
            "current_timestamp takes 0 arguments, got {:?}",
            input.arguments
        )))
    }
}

pub(crate) fn current_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    input.arguments.zero()?;
    let session_timezone = input.function_context.plan_config.session_timezone.clone();
    Ok(cast(
        to_session_local_timestamp(current_timestamp(&session_timezone), &session_timezone),
        DataType::Date32,
    ))
}

fn current_localtimestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_timezone = input.function_context.plan_config.session_timezone.clone();
    let timestamp = current_timestamp_microseconds(input)?;
    Ok(to_session_local_timestamp(timestamp, &session_timezone))
}

fn evaluate_current_time_precision(argument: &Expr) -> PlanResult<i32> {
    let string_literal = match argument {
        Expr::Literal(value, _) => value.try_as_str().flatten(),
        Expr::Cast(cast)
            if cast.field.data_type() == &DataType::Int32
                && let Expr::Literal(value, _) = cast.expr.as_ref() =>
        {
            value.try_as_str().flatten()
        }
        _ => None,
    };
    if let Some(value) = string_literal {
        return value.trim().parse::<i32>().map_err(|error| {
            PlanError::invalid(format!("cannot evaluate current_time precision: {error}"))
        });
    }

    match LiteralEvaluator::new().evaluate(&cast(argument.clone(), DataType::Int32)) {
        Ok(ScalarValue::Int32(Some(precision))) => Ok(precision),
        Ok(ScalarValue::Int32(None) | ScalarValue::Null) => Err(PlanError::invalid(
            "current_time precision must not be null",
        )),
        Ok(value) => Err(PlanError::invalid(format!(
            "current_time precision must be an integer, got {value}"
        ))),
        Err(error) => Err(PlanError::invalid(format!(
            "cannot evaluate current_time precision: {error}"
        ))),
    }
}

fn current_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    const DEFAULT_PRECISION: i32 = 6;

    let precision = match input.arguments.as_slice() {
        [] => DEFAULT_PRECISION,
        [argument] => {
            if argument.any_column_refs() || argument.contains_outer() || argument.is_volatile() {
                return Err(PlanError::invalid(
                    "current_time precision must be a foldable expression",
                ));
            }

            let data_type = argument.get_type(input.function_context.schema)?;
            if !data_type.is_numeric()
                && !matches!(
                    &data_type,
                    DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
            {
                return Err(PlanError::invalid(format!(
                    "current_time precision must have integer-compatible type, got {data_type}"
                )));
            }

            evaluate_current_time_precision(argument)?
        }
        arguments => {
            return Err(PlanError::invalid(format!(
                "current_time takes 0 or 1 arguments, got {}",
                arguments.len()
            )));
        }
    };
    if !(0..=6).contains(&precision) {
        return Err(PlanError::invalid(format!(
            "current_time precision must be between 0 and 6, got {precision}"
        )));
    }

    let data_type = match precision {
        0 => DataType::Time32(TimeUnit::Second),
        1..=3 => DataType::Time32(TimeUnit::Millisecond),
        4..=6 => DataType::Time64(TimeUnit::Microsecond),
        _ => unreachable!("current_time precision was validated above"),
    };

    let session_timezone = &input.function_context.plan_config.session_timezone;
    let time = cast(
        to_session_local_timestamp(current_timestamp(session_timezone), session_timezone),
        DataType::Time64(TimeUnit::Microsecond),
    );
    let factor = 10_i64.pow((6 - precision) as u32);
    let time = if factor == 1 {
        time
    } else {
        let micros = cast(time, DataType::Int64);
        cast(
            micros.clone() - micros % lit(factor),
            DataType::Time64(TimeUnit::Microsecond),
        )
    };
    let metadata = FieldMetadata::from(HashMap::from([(
        SAIL_SPARK_TIME_PRECISION_METADATA_KEY.to_string(),
        precision.to_string(),
    )]));
    Ok(cast(time, data_type).alias_with_metadata("current_time", Some(metadata)))
}

fn convert_tz(from_tz: Expr, to_tz: Expr, ts: Expr, classic: bool) -> Expr {
    ScalarUDF::from(ConvertTz::new(classic)).call(vec![from_tz, to_tz, ts])
}

/// A helper function for processing the input NTZ timestamp.
fn ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok((
            timezone_cast(ts, DataType::Timestamp(unit, None), session_timezone, false),
            unit,
        )),
        DataType::Timestamp(unit, None) => Ok((ts, unit)),
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
    let from_tz = coerce_string_argument(from_tz, input.function_context.schema, &session_tz)?;
    let to_tz = coerce_string_argument(to_tz, input.function_context.schema, &session_tz)?;
    let (ts, _unit) = ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    Ok(convert_tz(from_tz, to_tz, ts, true))
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
            let ts = convert_tz(lit(session_tz.to_string()), lit("UTC"), ts, true);
            (ts, unit)
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            let ts = cast(ts, DataType::Timestamp(unit, None));
            let ts = convert_tz(lit(session_tz.to_string()), lit("UTC"), ts, true);
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

fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, to_tz) = input.arguments.two()?;
    let to_tz = coerce_string_argument(to_tz, input.function_context.schema, &session_tz)?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(lit("UTC"), to_tz, ts, false);
    Ok(cast(
        cast(ts, DataType::Int64),
        DataType::Timestamp(unit, Some(Arc::from("UTC"))),
    ))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, from_tz) = input.arguments.two()?;
    let from_tz = coerce_string_argument(from_tz, input.function_context.schema, &session_tz)?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(from_tz, lit("UTC"), ts, false);
    Ok(cast(
        cast(ts, DataType::Int64),
        DataType::Timestamp(unit, Some(Arc::from("UTC"))),
    ))
}

fn make_timestamp_ltz(args: Vec<Expr>, session_tz: &Arc<str>, is_try: bool) -> PlanResult<Expr> {
    let ntz_ts = if args.len() == 2 || args.len() == 6 {
        ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args)
    } else if args.len() == 3 || args.len() == 7 {
        let mut args = args;
        let Some(from_tz) = args.pop() else {
            unreachable!()
        };
        let from_tz = ScalarUDF::from(SparkToUtf8::new(Arc::clone(session_tz))).call(vec![from_tz]);
        let ntz_ts = ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args);
        convert_tz(from_tz, lit(session_tz.to_string()), ntz_ts, true)
    } else {
        return Err(PlanError::invalid(format!(
            "{}make_timestamp_ltz requires 2, 3, 6 or 7 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )));
    };
    Ok(timezone_cast(
        ntz_ts,
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        session_tz,
        is_try,
    ))
}

fn make_timestamp_ntz(args: Vec<Expr>, is_try: bool) -> PlanResult<Expr> {
    if args.len() == 2 || args.len() == 6 {
        Ok(ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args))
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
    let part = coerce_string_argument(
        part,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    let date = session_local_timestamp_if_ltz(
        date,
        input.function_context.schema,
        &input.function_context.plan_config.session_timezone,
    )?;
    Ok(ScalarUDF::from(SparkDatePart::new()).call(vec![part, date]))
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
    let date1 = session_local_timestamp_if_ltz(
        date1,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;
    let date2 = session_local_timestamp_if_ltz(
        date2,
        function_context.schema,
        &function_context.plan_config.session_timezone,
    )?;

    // consts:
    let seconds_per_day: i64 = 24 * 60 * 60;
    let seconds_in_month = cast(lit(31 * seconds_per_day), DataType::Float64);

    // helper functions:
    let ensure_timestamp = |dt: Expr| match dt.get_type(function_context.schema) {
        Ok(DataType::Timestamp(time_unit, _tz)) => (dt.clone(), time_unit),
        _ => (
            cast(dt.clone(), DataType::Timestamp(TimeUnit::Microsecond, None)),
            TimeUnit::Microsecond,
        ),
    };

    let date_to_months =
        |dt: Expr| integer_part(dt.clone(), "YEAR") * lit(12) + integer_part(dt, "MONTH");

    let is_last_day = |dt: Expr| {
        ScalarUDF::from(SparkLastDay::new())
            .call(vec![cast(dt.clone(), DataType::Date32)])
            .eq(cast(dt, DataType::Date32))
    };

    let seconds_in_day = |dt: Expr, tu: TimeUnit| {
        (cast(dt.clone(), DataType::Int64)
            - cast(expr_fn::date_trunc(lit("DAY"), dt), DataType::Int64))
            / lit(time_unit_to_multiplier(&tu))
    };

    // prerequisites
    let (date1, tu1) = ensure_timestamp(date1.clone());
    let (date2, tu2) = ensure_timestamp(date2.clone());

    // calculations:
    let days1 = integer_part(date1.clone(), "DAY");
    let days2 = integer_part(date2.clone(), "DAY");

    let month_diff = cast(
        date_to_months(date1.clone()) - date_to_months(date2.clone()),
        DataType::Float64,
    );

    let seconds_diff = (days1.clone() - days2.clone()) * lit(seconds_per_day)
        + seconds_in_day(date1.clone(), tu1)
        - seconds_in_day(date2.clone(), tu2);

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

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

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
            IntervalValue::Microsecond { microseconds } => Ok(microseconds),
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
fn window_field_type(time_type: &DataType) -> PlanResult<DataType> {
    Ok(match time_type {
        DataType::Timestamp(_, None) => DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(_, Some(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
        // Spark casts dates and strings to `TimestampType` (session time zone), so a
        // date becomes midnight in the session time zone, not a naive timestamp.
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        other => {
            return Err(PlanError::invalid(format!(
                "window requires a timestamp time column, got {other:?}"
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
    let field_type = window_field_type(&time.get_type(schema)?)?;
    let time_ts = match field_type {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            cast(time, DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        _ => coerce_to_ltz(
            time,
            TimeUnit::Microsecond,
            schema,
            &session_tz,
            input.function_context.plan_config.ansi_mode,
            false,
        )?,
    };
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
        ("curdate", F::custom(current_date)),
        ("current_date", F::custom(current_date)),
        ("current_time", F::custom(current_time)),
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        (
            "date_add",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("date_diff", F::custom(datediff)),
        (
            "date_format",
            F::custom(|input| match input.arguments.len() {
                2 => {
                    let timezone = input.function_context.plan_config.session_timezone.clone();
                    Ok(date_format_with_args(input.arguments, timezone.to_string()))
                }
                _ => Err(PlanError::invalid("date_format requires 2 arguments")),
            }),
        ),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::custom(date_part)),
        (
            "date_sub",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Minus)),
        ),
        ("date_trunc", F::custom(date_trunc)),
        (
            "dateadd",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("datediff", F::custom(datediff)),
        ("datepart", F::custom(date_part)),
        (
            "day",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "DAY"))),
        ),
        (
            "dayname",
            F::custom(|input| session_local_unary(input, |arg| expr_fn::to_char(arg, lit("%a")))),
        ),
        (
            "dayofmonth",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "DAY"))),
        ),
        (
            "dayofweek",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "DOW") + lit(1))),
        ),
        (
            "dayofyear",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "DOY"))),
        ),
        ("extract", F::custom(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::custom(from_utc_timestamp)),
        (
            "hour",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "HOUR"))),
        ),
        (
            "last_day",
            F::custom(|input| {
                session_local_unary(input, |arg| {
                    ScalarUDF::from(SparkLastDay::new()).call(vec![arg])
                })
            }),
        ),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::ternary(make_date)),
        ("make_dt_interval", F::udf(SparkMakeDtInterval::new())),
        ("make_interval", F::udf(SparkMakeInterval::new())),
        ("make_time", F::udf(SparkMakeTime::new())),
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
        ("make_ym_interval", F::udf(SparkMakeYmInterval::new())),
        (
            "minute",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "MINUTE"))),
        ),
        (
            "month",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "MONTH"))),
        ),
        (
            "monthname",
            F::custom(|input| session_local_unary(input, |arg| expr_fn::to_char(arg, lit("%b")))),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        (
            "quarter",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "QUARTER"))),
        ),
        (
            "second",
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "SECOND"))),
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
        ("try_make_interval", F::unknown("try_make_interval")),
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
        ("time_diff", F::udf(SparkTimeDiff::new())),
        ("time_trunc", F::udf(SparkTimeTrunc::new())),
        (
            "unix_date",
            F::custom(|input| {
                session_local_unary(input, |arg| {
                    cast(cast(arg, DataType::Date32), DataType::Int32)
                })
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
            F::custom(|input| session_local_unary(input, |arg| integer_part(arg, "DOW") - lit(1))),
        ),
        (
            "weekofyear",
            F::custom(|input| {
                session_local_unary(input, |arg| {
                    cast(expr_fn::to_char(arg, lit("%V")), DataType::Int32)
                })
            }),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        (
            "year",
            F::custom(|input| {
                session_local_unary(input, |arg| {
                    ScalarUDF::from(SparkYear::new()).call(vec![arg])
                })
            }),
        ),
        (
            "years",
            F::custom(|input| session_local_unary(input, years)),
        ),
    ]
}
