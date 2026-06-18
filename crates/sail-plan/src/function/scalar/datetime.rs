use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{cast, lit, try_cast, when, BinaryExpr, ExprSchemable, Operator, ScalarUDF};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions::expr_fn::to_time;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common::datetime::time_unit_to_multiplier;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_to_chrono_fmt::SparkToChronoFmt;
use sail_function::scalar::datetime::spark_try_to_timestamp::SparkTryToTimestamp;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

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

fn trunc(date: Expr, part: Expr) -> Expr {
    cast(
        expr_fn::date_trunc(trunc_part_conversion(part), date),
        DataType::Date32,
    )
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    let truncated =
        ScalarUDF::from(SparkDateTrunc::new()).call(vec![trunc_part_conversion(part), timestamp]);
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
            )))
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

fn timestampadd_interval(unit: &str, quantity: Expr) -> PlanResult<Expr> {
    let zero_i32 = || lit(0_i32);
    let zero_f64 = || lit(0_f64);
    let quantity_i32 = || cast(quantity.clone(), DataType::Int32);
    let quantity_f64 = || cast(quantity.clone(), DataType::Float64);
    let make_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeInterval::new()).call(args);
    let make_dt_interval = |args: Vec<Expr>| ScalarUDF::from(SparkMakeDtInterval::new()).call(args);

    match unit {
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
            "timestampadd does not support interval unit type '{unit}'"
        ))),
    }
}

fn timestampadd(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (unit, quantity, timestamp) = input.arguments.three()?;
    let unit = match &unit {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.to_uppercase(),
        Expr::Column(col) => col.name().to_uppercase(),
        _ => {
            return Err(PlanError::invalid(
                "timestampadd unit must be a string literal or keyword",
            ))
        }
    };
    let interval = timestampadd_interval(&unit, quantity)?;
    Ok(cast(
        timestamp,
        DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(input.function_context.plan_config.session_timezone.clone()),
        ),
    ) + interval)
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

fn datediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let args = input.arguments;
    match args.len() {
        2 => {
            let [start, end] = <[Expr; 2]>::try_from(args)
                .map_err(|_| PlanError::invalid("datediff requires 2 or 3 arguments"))?;
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
                    ))
                }
            };
            match unit_str.as_str() {
                "DAY" => Ok(date_days_arithmetic(end, start, Operator::Minus)),
                "HOUR" | "MINUTE" | "SECOND" | "MONTH" | "YEAR" | "WEEK" | "QUARTER" => {
                    let start_ts = cast(start, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let end_ts = cast(end, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let diff_seconds = cast(
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(cast(end_ts, DataType::Int64)),
                            op: Operator::Minus,
                            right: Box::new(cast(start_ts, DataType::Int64)),
                        }),
                        DataType::Int64,
                    );
                    let divisor = match unit_str.as_str() {
                        "SECOND" => 1_000_000i64,
                        "MINUTE" => 60_000_000i64,
                        "HOUR" => 3_600_000_000i64,
                        "WEEK" => 7 * 24 * 3_600_000_000i64,
                        "MONTH" => 30 * 24 * 3_600_000_000i64,
                        "YEAR" => 365 * 24 * 3_600_000_000i64,
                        "QUARTER" => 91 * 24 * 3_600_000_000i64,
                        _ => 1i64,
                    };
                    Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(diff_seconds),
                        op: Operator::Divide,
                        right: Box::new(lit(divisor)),
                    }))
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

fn current_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    input.arguments.zero()?;
    Ok(session_tz)
}

fn to_chrono_fmt(format: Expr) -> Expr {
    ScalarUDF::from(SparkToChronoFmt::new()).call(vec![format])
}

fn to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        // If format is not supplied, the function is a synonym for cast(expr AS DATE).
        crate::function::scalar::conversion::cast_to_date(input)
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let expr_type = expr.get_type(input.function_context.schema);
        if let Ok(DataType::Timestamp(_, _)) = expr_type {
            let expr = expr_fn::to_local_time(vec![expr]);
            return Ok(cast(expr, DataType::Date32)); // In case of data type timestamp, ignore format
        }
        let expr = match expr_type {
            Ok(_other) => expr,
            Err(_) => cast(expr, DataType::Utf8), // In case of error, cast to string
        };
        let format = to_chrono_fmt(format);
        Ok(expr_fn::to_date(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_date requires 1 or 2 arguments"))
    }
}

fn unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let timezone = input.function_context.plan_config.session_timezone.clone();
    if input.arguments.is_empty() {
        let expr = ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Second)).call(vec![]);
        Ok(cast(expr, DataType::Int64))
    } else if input.arguments.len() == 1 {
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone)).call(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format);
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone)).call(vec![expr, format]))
    } else {
        Err(PlanError::invalid(
            "unix_timestamp requires 1 or 2 arguments",
        ))
    }
}

fn to_unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
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

pub(super) fn date_format(expr: Expr, format: Expr) -> Expr {
    // Handle standalone fractional seconds format (e.g., 'SSS' for milliseconds).
    // Chrono's %.Nf always includes a leading dot (e.g., ".000"), so for standalone
    // S-patterns we strip the dot using substr.
    if let Expr::Literal(ref sv, _) = &format {
        if let Some(Some(fmt)) = sv.try_as_str() {
            if !fmt.is_empty() && fmt.chars().all(|c| c == 'S') {
                let n = fmt.len();
                let chrono_fmt = format!("%.{n}f");
                let result = expr_fn::to_char(expr, lit(chrono_fmt));
                return expr_fn::substr(result, lit(2i64));
            }
        }
    }
    let format = to_chrono_fmt(format);
    expr_fn::to_char(expr, format)
}

fn timestamp_data_type(input: &ScalarFunctionInput, timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn timestamp_null(input: &ScalarFunctionInput, timestamp_ntz: bool) -> Expr {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    lit(ScalarValue::TimestampMicrosecond(None, timezone))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(value, _) if value.is_null())
}

fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        let expr = match expr.get_type(input.function_context.schema)? {
            DataType::Timestamp(_, Some(_)) => expr_fn::to_local_time(vec![expr]),
            _ => expr,
        };
        Ok(cast(expr, data_type))
    } else if input.arguments.len() == 2 {
        let null = timestamp_null(&input, timestamp_ntz);
        let (expr, format) = input.arguments.two()?;
        if is_null_literal(&expr) || is_null_literal(&format) {
            return Ok(null);
        }
        let format = to_chrono_fmt(format);
        Ok(cast(
            expr_fn::to_timestamp_micros(vec![expr, format]),
            data_type,
        ))
    } else {
        Err(PlanError::invalid("to_timestamp requires 1 or 2 arguments"))
    }
}

fn try_to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        let expr = match expr.get_type(input.function_context.schema)? {
            DataType::Timestamp(_, Some(_)) => expr_fn::to_local_time(vec![expr]),
            _ => expr,
        };
        Ok(try_cast(expr, data_type))
    } else if input.arguments.len() == 2 {
        let null = timestamp_null(&input, timestamp_ntz);
        let (expr, format) = input.arguments.two()?;
        if is_null_literal(&expr) || is_null_literal(&format) {
            return Ok(null);
        }
        let format = to_chrono_fmt(format);
        Ok(cast(
            ScalarUDF::from(SparkTryToTimestamp::new()).call(vec![expr, format]),
            data_type,
        ))
    } else {
        Err(PlanError::invalid(
            "try_to_timestamp requires 1 or 2 arguments",
        ))
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
    let format = to_chrono_fmt(format);
    let expr = cast(expr, DataType::Timestamp(TimeUnit::Second, Some(timezone)));
    Ok(expr_fn::to_char(expr, format))
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

fn convert_tz(from_tz: Expr, to_tz: Expr, ts: Expr, classic: bool) -> Expr {
    ScalarUDF::from(ConvertTz::new(classic)).call(vec![from_tz, to_tz, ts])
}

/// A helper function for processing the input NTZ timestamp.
fn ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => Ok((expr_fn::to_local_time(vec![ts]), unit)),
        DataType::Timestamp(unit, None) => Ok((ts, unit)),
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let is_try = !ansi_mode;
            let ts = ScalarUDF::from(SparkTimestamp::try_new(None, is_try)?).call(vec![ts]);
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
    let (ts, _unit) = ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
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
) -> PlanResult<(Expr, TimeUnit)> {
    let (ts, unit) = match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => (ts, unit),
        DataType::Timestamp(unit, None) => {
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts = cast(ts, DataType::Timestamp(unit, Some(session_tz.clone())));
            (ts, unit)
        }
        x => {
            return Err(PlanError::invalid(format!(
                "invalid UTC NTZ timestamp type: {x:?}"
            )))
        }
    };
    let ts = cast(ts, DataType::Timestamp(unit, None));
    Ok((ts, unit))
}

fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, to_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(ts, input.function_context.schema, &session_tz)?;
    let ts = convert_tz(lit("UTC"), to_tz, ts, false);
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, from_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(ts, input.function_context.schema, &session_tz)?;
    let ts = convert_tz(from_tz, lit("UTC"), ts, false);
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn make_ym_interval(args: Vec<Expr>) -> PlanResult<Expr> {
    let (years, months) = if args.len() == 2 {
        args.two()?
    } else {
        (args.one()?, lit(0_i32))
    };
    Ok(ScalarUDF::from(SparkMakeYmInterval::new()).call(vec![years, months]))
}

fn make_timestamp_ltz(args: Vec<Expr>, session_tz: &Arc<str>, is_try: bool) -> PlanResult<Expr> {
    let ntz_ts = if args.len() == 2 || args.len() == 6 {
        ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args)
    } else if args.len() == 3 || args.len() == 7 {
        let mut args = args;
        let Some(from_tz) = args.pop() else {
            unreachable!()
        };
        let ntz_ts = ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args);
        convert_tz(from_tz, lit(session_tz.to_string()), ntz_ts, true)
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

fn date_part(part: Expr, date: Expr) -> Expr {
    ScalarUDF::from(SparkDatePart::new()).call(vec![part, date])
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
            )))
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
    let field_type = window_field_type(&time.get_type(schema)?, &session_tz)?;
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
        other => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {other:?}"
            )))
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
        (
            "date_add",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("date_diff", F::custom(datediff)),
        ("date_format", F::binary(date_format)),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::binary(date_part)),
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
        ("datepart", F::binary(date_part)),
        ("day", F::unary(|arg| integer_part(arg, "DAY"))),
        ("dayname", F::unary(|arg| expr_fn::to_char(arg, lit("%a")))),
        ("dayofmonth", F::unary(|arg| integer_part(arg, "DAY"))),
        (
            "dayofweek",
            F::unary(|arg| integer_part(arg, "DOW") + lit(1)),
        ),
        ("dayofyear", F::unary(|arg| integer_part(arg, "DOY"))),
        ("extract", F::binary(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::custom(from_utc_timestamp)),
        ("hour", F::unary(|arg| integer_part(arg, "HOUR"))),
        ("last_day", F::udf(SparkLastDay::new())),
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
        ("make_ym_interval", F::var_arg(make_ym_interval)),
        ("minute", F::unary(|arg| integer_part(arg, "MINUTE"))),
        ("month", F::unary(|arg| integer_part(arg, "MONTH"))),
        (
            "monthname",
            F::unary(|arg| expr_fn::to_char(arg, lit("%b"))),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        ("quarter", F::unary(|arg| integer_part(arg, "QUARTER"))),
        ("second", F::unary(|arg| integer_part(arg, "SECOND"))),
        ("session_window", F::unknown("session_window")),
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
        ("timestampdiff", F::custom(datediff)),
        ("to_date", F::custom(to_date)),
        ("to_time", F::var_arg(to_time)),
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
        ("trunc", F::binary(trunc)),
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
        ("weekday", F::unary(|arg| integer_part(arg, "DOW") - lit(1))),
        (
            "weekofyear",
            F::unary(|arg| cast(expr_fn::to_char(arg, lit("%V")), DataType::Int32)),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        ("year", F::udf(SparkYear::new())),
        ("years", F::unary(years)),
    ]
}
