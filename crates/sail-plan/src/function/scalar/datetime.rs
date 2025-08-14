use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{cast, lit, BinaryExpr, Operator, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::datetime::convert_tz::ConvertTz;
use crate::extension::function::datetime::spark_date_part::SparkDatePart;
use crate::extension::function::datetime::spark_last_day::SparkLastDay;
use crate::extension::function::datetime::spark_make_timestamp::SparkMakeTimestampNtz;
use crate::extension::function::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use crate::extension::function::datetime::spark_next_day::SparkNextDay;
use crate::extension::function::datetime::spark_to_chrono_fmt::SparkToChronoFmt;
use crate::extension::function::datetime::spark_try_to_timestamp::SparkTryToTimestamp;
use crate::extension::function::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use crate::extension::function::datetime::timestamp_now::TimestampNow;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        expr_fn::date_part(lit(part.to_uppercase()), expr),
        DataType::Int32,
    )
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
    Ok(cast(
        expr_fn::date_trunc(trunc_part_conversion(part), date),
        DataType::Date32,
    ))
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    Ok(cast(
        expr_fn::date_trunc(trunc_part_conversion(part), timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ))
}

fn interval_arithmetic(input: ScalarFunctionInput, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = input.arguments.two()?;
    let date = Expr::Cast(expr::Cast {
        expr: Box::new(date),
        data_type: DataType::Date32,
    });
    let interval = match unit.to_lowercase().as_str() {
        "years" | "year" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(years)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(years, 0))),
                metadata,
            ),
            _ => Expr::Cast(expr::Cast {
                expr: Box::new(format_interval(interval, "years")),
                data_type: DataType::Interval(IntervalUnit::YearMonth),
            }),
        },
        "months" | "month" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(months)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(0, months))),
                metadata,
            ),
            _ => Expr::Cast(expr::Cast {
                expr: Box::new(format_interval(interval, "months")),
                data_type: DataType::Interval(IntervalUnit::YearMonth),
            }),
        },
        "days" | "day" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(days)), metadata) => Expr::Literal(
                ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(days, 0))),
                metadata,
            ),
            _ => Expr::Cast(expr::Cast {
                expr: Box::new(format_interval(interval, "days")),
                data_type: DataType::Interval(IntervalUnit::DayTime),
            }),
        },
        _ => {
            return Err(PlanError::invalid(format!(
                "add_interval does not support interval unit type '{unit}'"
            )))
        }
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

fn to_chrono_fmt(format: Expr) -> PlanResult<Expr> {
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkToChronoFmt::new())),
        args: vec![format],
    }))
}

fn to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        Ok(expr_fn::to_date(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(expr_fn::to_date(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_date requires 1 or 2 arguments"))
    }
}

fn unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let timezone = input.function_context.plan_config.session_timezone.clone();
    if input.arguments.is_empty() {
        let expr = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Second,
            ))),
            args: vec![],
        });
        Ok(cast(expr, DataType::Int64))
    } else if input.arguments.len() == 1 {
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone))),
            args: input.arguments,
        }))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone))),
            args: vec![expr, format],
        }))
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

fn date_format(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (expr, format) = input.arguments.two()?;
    let format = to_chrono_fmt(format)?;
    Ok(expr_fn::to_char(expr, format))
}

fn to_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        Ok(Expr::Cast(expr::Cast::new(
            Box::new(input.arguments.one()?),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(expr_fn::to_timestamp_micros(vec![expr, format]))
    } else {
        Err(PlanError::invalid("to_timestamp requires 1 or 2 arguments"))
    }
}

fn try_to_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 1 {
        Ok(Expr::TryCast(expr::TryCast::new(
            Box::new(input.arguments.one()?),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkTryToTimestamp::new())),
            args: vec![expr, format],
        }))
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
        Ok((
            expr,
            lit(ScalarValue::Utf8(Some("yyyy-MM-dd HH:mm:ss".to_string()))),
        ))
    } else if input.arguments.len() == 2 {
        input.arguments.two()
    } else {
        return Err(PlanError::invalid(
            "from_unixtime requires 1 or 2 arguments",
        ));
    }?;

    let timezone = input.function_context.plan_config.session_timezone.clone();
    let format = to_chrono_fmt(format)?;
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
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Microsecond,
            ))),
            args: vec![],
        }))
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

fn convert_tz(from_tz: Expr, to_tz: Expr, ts: Expr) -> Expr {
    Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(ConvertTz::new())),
        args: vec![from_tz, to_tz, ts],
    })
}

fn convert_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    let args = input.arguments;
    let (from_tz, to_tz, ts) = match args.len() {
        3 => Ok(args.three()?),
        2 => {
            let (to_tz, ts) = args.two()?;
            Ok((session_tz, to_tz, ts))
        }
        _ => Err(PlanError::invalid(format!(
            "convert_timezone takes 2 or three arguments, got {args:?}"
        ))),
    }?;
    Ok(convert_tz(from_tz, to_tz, ts))
}

fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    let (ts, to_tz) = input.arguments.two()?;
    Ok(convert_tz(session_tz, to_tz, ts))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    let (ts, from_tz) = input.arguments.two()?;
    Ok(convert_tz(from_tz, session_tz, ts))
}

fn make_ym_interval(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (years, months) = if input.arguments.len() == 2 {
        input.arguments.two()?
    } else {
        (input.arguments.one()?, lit(ScalarValue::Int32(Some(0))))
    };
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkMakeYmInterval::new())),
        args: vec![years, months],
    }))
}

fn make_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.len() == 6 {
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new())),
            args: input.arguments,
        }))
    } else if input.arguments.len() == 7 {
        let session_tz = session_timezone(&input);
        let mut args = input.arguments;
        let from_tz = args.pop().ok_or_else(|| {
            PlanError::invalid(
                "make_timestamp: empty args array with len = 7, should be unreachable",
            )
        })?;

        let ntz_ts = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new())),
            args,
        });

        Ok(convert_tz(from_tz, session_tz, ntz_ts))
    } else {
        Err(PlanError::invalid(format!(
            "make_timestamp requires 6 or 7 arguments, got {:?}",
            input.arguments
        )))
    }
}

fn date_part(part: Expr, date: Expr) -> Expr {
    Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkDatePart::new())),
        args: vec![part, date],
    })
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
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        (
            "date_add",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        (
            "date_diff",
            F::binary(|start, end| date_days_arithmetic(start, end, Operator::Minus)),
        ),
        ("date_format", F::custom(date_format)),
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
        (
            "datediff",
            F::binary(|start, end| date_days_arithmetic(start, end, Operator::Minus)),
        ),
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
        ("make_dt_interval", F::unknown("make_dt_interval")),
        ("make_interval", F::unknown("make_interval")),
        ("make_timestamp", F::custom(make_timestamp)),
        ("make_timestamp_ltz", F::custom(make_timestamp)),
        ("make_timestamp_ntz", F::udf(SparkMakeTimestampNtz::new())),
        ("make_ym_interval", F::custom(make_ym_interval)),
        ("minute", F::unary(|arg| integer_part(arg, "MINUTE"))),
        ("month", F::unary(|arg| integer_part(arg, "MONTH"))),
        (
            "monthname",
            F::unary(|arg| expr_fn::to_char(arg, lit("%b"))),
        ),
        ("months_between", F::unknown("months_between")),
        ("next_day", F::udf(SparkNextDay::new())),
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
        ("to_date", F::custom(to_date)),
        ("to_timestamp", F::custom(to_timestamp)),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        ("to_timestamp_ltz", F::custom(to_timestamp)),
        ("to_timestamp_ntz", F::custom(to_timestamp)),
        ("to_unix_timestamp", F::custom(to_unix_timestamp)),
        ("to_utc_timestamp", F::custom(to_utc_timestamp)),
        ("trunc", F::custom(trunc)),
        ("try_to_timestamp", F::custom(try_to_timestamp)),
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
        ("window", F::unknown("window")),
        ("window_time", F::unknown("window_time")),
        ("year", F::unary(|arg| integer_part(arg, "YEAR"))),
    ]
}
