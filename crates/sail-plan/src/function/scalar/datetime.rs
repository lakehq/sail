use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{lit, BinaryExpr, Operator, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::spark_unix_timestamp::SparkUnixTimestamp;
use crate::extension::function::spark_weekofyear::SparkWeekOfYear;
use crate::extension::function::timestamp_now::TimestampNow;
use crate::function::common::{Function, FunctionContext};
use crate::utils::{spark_datetime_format_to_chrono_strftime, ItemTaker};

fn integer_part(expr: Expr, part: &str) -> Expr {
    let part = lit(ScalarValue::Utf8(Some(part.to_uppercase())));
    Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::date_part(part, expr)),
        data_type: DataType::Int32,
    })
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

fn trunc(args: Vec<Expr>, _function_context: &FunctionContext) -> PlanResult<Expr> {
    let (date, part) = args.two()?;
    Ok(Expr::Cast(expr::Cast::new(
        Box::new(expr_fn::date_trunc(trunc_part_conversion(part), date)),
        DataType::Date32,
    )))
}

fn date_trunc(args: Vec<Expr>, _function_context: &FunctionContext) -> PlanResult<Expr> {
    let (part, timestamp) = args.two()?;
    Ok(Expr::Cast(expr::Cast::new(
        Box::new(expr_fn::date_trunc(trunc_part_conversion(part), timestamp)),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    )))
}

fn interval_arithmetic(args: Vec<Expr>, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = args.two()?;
    let date = Expr::Cast(expr::Cast {
        expr: Box::new(date),
        data_type: DataType::Date32,
    });
    let interval = match unit.to_lowercase().as_str() {
        "years" | "year" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(years))) => lit(ScalarValue::IntervalYearMonth(
                Some(IntervalYearMonthType::make_value(years, 0)),
            )),
            _ => Expr::Cast(expr::Cast {
                expr: Box::new(format_interval(interval, "years")),
                data_type: DataType::Interval(IntervalUnit::YearMonth),
            }),
        },
        "months" | "month" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(months))) => lit(ScalarValue::IntervalYearMonth(
                Some(IntervalYearMonthType::make_value(0, months)),
            )),
            _ => Expr::Cast(expr::Cast {
                expr: Box::new(format_interval(interval, "months")),
                data_type: DataType::Interval(IntervalUnit::YearMonth),
            }),
        },
        "days" | "day" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(days))) => lit(ScalarValue::IntervalDayTime(
                Some(IntervalDayTimeType::make_value(days, 0)),
            )),
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
    if matches!(&year, Expr::Literal(ScalarValue::Null))
        || matches!(&month, Expr::Literal(ScalarValue::Null))
        || matches!(&day, Expr::Literal(ScalarValue::Null))
    {
        Expr::Literal(ScalarValue::Null)
    } else {
        expr_fn::make_date(year, month, day)
    }
}

fn date_days_arithmetic(dt1: Expr, dt2: Expr, op: Operator) -> Expr {
    let (dt1, dt2) = match (&dt1, &dt2) {
        (Expr::Literal(ScalarValue::Date32(_)), Expr::Literal(ScalarValue::Date32(_))) => {
            (dt1, dt2)
        }
        _ => (
            Expr::Cast(expr::Cast {
                expr: Box::new(dt1),
                data_type: DataType::Date32,
            }),
            Expr::Cast(expr::Cast {
                expr: Box::new(dt2),
                data_type: DataType::Date32,
            }),
        ),
    };
    let dt1 = Expr::Cast(expr::Cast {
        expr: Box::new(dt1),
        data_type: DataType::Int64,
    });
    let dt2 = Expr::Cast(expr::Cast {
        expr: Box::new(dt2),
        data_type: DataType::Int64,
    });
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dt1),
        op,
        right: Box::new(dt2),
    })
}

fn current_timezone(args: Vec<Expr>, function_context: &FunctionContext) -> PlanResult<Expr> {
    args.zero()?;
    Ok(Expr::Literal(ScalarValue::Utf8(Some(
        function_context.plan_config().session_timezone.clone(),
    ))))
}

fn to_chrono_fmt(format: Expr) -> PlanResult<Expr> {
    // FIXME: Implement UDFs for all callers of this function so that we have `format` as a string
    //  instead of some arbitrary expression
    match format {
        Expr::Literal(ScalarValue::Utf8(Some(format))) => {
            let format = spark_datetime_format_to_chrono_strftime(&format)?;
            Ok(lit(ScalarValue::Utf8(Some(format))))
        }
        _ => Ok(format),
    }
}

fn to_date(args: Vec<Expr>, _function_context: &FunctionContext) -> PlanResult<Expr> {
    if args.len() == 1 {
        Ok(expr_fn::to_date(args))
    } else if args.len() == 2 {
        let (expr, format) = args.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(expr_fn::to_date(vec![expr, format]))
    } else {
        return Err(PlanError::invalid("to_date requires 1 or 2 arguments"));
    }
}

fn unix_timestamp(args: Vec<Expr>, function_context: &FunctionContext) -> PlanResult<Expr> {
    let timezone: Arc<str> = function_context
        .plan_config()
        .session_timezone
        .clone()
        .into();
    if args.is_empty() {
        let expr = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Second,
            ))),
            args: vec![],
        });
        Ok(Expr::Cast(expr::Cast::new(Box::new(expr), DataType::Int64)))
    } else if args.len() == 1 {
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone))),
            args,
        }))
    } else if args.len() == 2 {
        let (expr, format) = args.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone))),
            args: vec![expr, format],
        }))
    } else {
        return Err(PlanError::invalid(
            "unix_timestamp requires 1 or 2 arguments",
        ));
    }
}

fn date_format(args: Vec<Expr>, _function_context: &FunctionContext) -> PlanResult<Expr> {
    let (expr, format) = args.two()?;
    let format = to_chrono_fmt(format)?;
    Ok(expr_fn::to_char(expr, format))
}

fn to_timestamp(args: Vec<Expr>, _function_context: &FunctionContext) -> PlanResult<Expr> {
    if args.len() == 1 {
        Ok(Expr::Cast(expr::Cast::new(
            Box::new(args.one()?),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )))
    } else if args.len() == 2 {
        let (expr, format) = args.two()?;
        let format = to_chrono_fmt(format)?;
        Ok(expr_fn::to_timestamp_micros(vec![expr, format]))
    } else {
        return Err(PlanError::invalid("to_timestamp requires 1 or 2 arguments"));
    }
}

fn from_unixtime(args: Vec<Expr>, function_context: &FunctionContext) -> PlanResult<Expr> {
    let (expr, format) = if args.len() == 1 {
        let expr = args.one()?;
        // default format is "yyyy-MM-dd HH:mm:ss"
        Ok((
            expr,
            lit(ScalarValue::Utf8(Some("yyyy-MM-dd HH:mm:ss".to_string()))),
        ))
    } else if args.len() == 2 {
        args.two()
    } else {
        return Err(PlanError::invalid(
            "from_unixtime requires 1 or 2 arguments",
        ));
    }?;

    let timezone: Arc<str> = function_context
        .plan_config()
        .session_timezone
        .clone()
        .into();
    let format = to_chrono_fmt(format)?;
    let expr = Expr::Cast(expr::Cast::new(
        Box::new(expr),
        DataType::Timestamp(TimeUnit::Second, Some(timezone)),
    ));
    Ok(expr_fn::to_char(expr, format))
}

fn weekofyear(args: Vec<Expr>, function_context: &FunctionContext) -> PlanResult<Expr> {
    if args.len() == 1 {
        let timezone: Arc<str> = function_context
            .plan_config()
            .session_timezone
            .clone()
            .into();
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkWeekOfYear::new(timezone))),
            args,
        }))
    } else {
        Err(PlanError::invalid(format!(
            "weekofyear requires 1 argument, got {args:?}"
        )))
    }
}

fn unix_time_unit(
    args: Vec<Expr>,
    function_context: &FunctionContext,
    time_unit: TimeUnit,
) -> PlanResult<Expr> {
    let arg = args.one()?;
    Ok(Expr::Cast(expr::Cast::new(
        Box::new(Expr::Cast(expr::Cast::new(
            Box::new(arg),
            DataType::Timestamp(
                time_unit,
                Some(
                    function_context
                        .plan_config()
                        .session_timezone
                        .clone()
                        .into(),
                ),
            ),
        ))),
        DataType::Int64,
    )))
}

fn current_timestamp_microseconds(
    args: Vec<Expr>,
    function_context: &FunctionContext,
) -> PlanResult<Expr> {
    if args.is_empty() {
        let timezone: Arc<str> = function_context
            .plan_config()
            .session_timezone
            .clone()
            .into();
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Microsecond,
            ))),
            args: vec![],
        }))
    } else {
        Err(PlanError::invalid(format!(
            "current_timestamp takes 0 arguments, got {args:?}"
        )))
    }
}

fn current_localtimestamp_microseconds(
    args: Vec<Expr>,
    function_context: &FunctionContext,
) -> PlanResult<Expr> {
    let expr = current_timestamp_microseconds(args, function_context)?;
    Ok(expr_fn::to_local_time(vec![expr]))
}

// FIXME: Spark displays dates and timestamps according to the session time zone.
//  We should be setting the DataFusion config `datafusion.execution.time_zone`
//  and casting any datetime functions that don't use the DataFusion config.
pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|args, _config| interval_arithmetic(args, "years", Operator::Plus)),
        ),
        (
            "add_months",
            F::custom(|args, _config| interval_arithmetic(args, "months", Operator::Plus)),
        ),
        (
            "add_days",
            F::custom(|args, _config| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        ("convert_timezone", F::unknown("convert_timezone")),
        ("curdate", F::nullary(expr_fn::current_date)),
        ("current_date", F::nullary(expr_fn::current_date)),
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        (
            "date_add",
            F::custom(|args, _config| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        (
            "date_diff",
            F::binary(|start, end| date_days_arithmetic(start, end, Operator::Minus)),
        ),
        ("date_format", F::custom(date_format)),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::binary(expr_fn::date_part)),
        (
            "date_sub",
            F::custom(|args, _config| interval_arithmetic(args, "days", Operator::Minus)),
        ),
        ("date_trunc", F::custom(date_trunc)),
        (
            "dateadd",
            F::custom(|args, _config| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        (
            "datediff",
            F::binary(|start, end| date_days_arithmetic(start, end, Operator::Minus)),
        ),
        ("datepart", F::binary(expr_fn::date_part)),
        ("day", F::unary(|arg| integer_part(arg, "DAY"))),
        ("dayofmonth", F::unary(|arg| integer_part(arg, "DAY"))),
        (
            "dayofweek",
            F::unary(|arg| {
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(integer_part(arg, "DOW")),
                    Operator::Plus,
                    Box::new(lit(ScalarValue::Int32(Some(1)))),
                ))
            }),
        ),
        ("dayofyear", F::unary(|arg| integer_part(arg, "DOY"))),
        ("extract", F::binary(expr_fn::date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::unknown("from_utc_timestamp")),
        ("hour", F::unary(|arg| integer_part(arg, "HOUR"))),
        ("last_day", F::unknown("last_day")),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::ternary(make_date)),
        ("make_dt_interval", F::unknown("make_dt_interval")),
        ("make_interval", F::unknown("make_interval")),
        ("make_timestamp", F::unknown("make_timestamp")),
        ("make_timestamp_ltz", F::unknown("make_timestamp_ltz")),
        ("make_timestamp_ntz", F::unknown("make_timestamp_ntz")),
        ("make_ym_interval", F::unknown("make_ym_interval")),
        ("minute", F::unary(|arg| integer_part(arg, "MINUTE"))),
        ("month", F::unary(|arg| integer_part(arg, "MONTH"))),
        ("months_between", F::unknown("months_between")),
        ("next_day", F::unknown("next_day")),
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
                Expr::Cast(expr::Cast::new(
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Int64))),
                        Operator::Multiply,
                        Box::new(lit(ScalarValue::Int64(Some(1_000)))),
                    ))),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ))
            }),
        ),
        (
            "timestamp_seconds",
            F::unary(|arg| {
                Expr::Cast(expr::Cast::new(
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Int64))),
                        Operator::Multiply,
                        Box::new(lit(ScalarValue::Int64(Some(1_000_000)))),
                    ))),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ))
            }),
        ),
        ("to_date", F::custom(to_date)),
        ("to_timestamp", F::custom(to_timestamp)),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/3.5.4/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/3.5.4/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        ("to_timestamp_ltz", F::custom(to_timestamp)),
        ("to_timestamp_ntz", F::custom(to_timestamp)),
        ("to_unix_timestamp", F::unknown("to_unix_timestamp")),
        ("to_utc_timestamp", F::unknown("to_utc_timestamp")),
        ("trunc", F::custom(trunc)),
        ("try_to_timestamp", F::unknown("try_to_timestamp")),
        (
            "unix_date",
            F::unary(|arg| {
                Expr::Cast(expr::Cast::new(
                    Box::new(Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Date32))),
                    DataType::Int32,
                ))
            }),
        ),
        (
            "unix_micros",
            F::custom(|args, function_context| {
                unix_time_unit(args, function_context, TimeUnit::Microsecond)
            }),
        ),
        (
            "unix_millis",
            F::custom(|args, function_context| {
                unix_time_unit(args, function_context, TimeUnit::Millisecond)
            }),
        ),
        (
            "unix_seconds",
            F::custom(|args, function_context| {
                unix_time_unit(args, function_context, TimeUnit::Second)
            }),
        ),
        ("unix_timestamp", F::custom(unix_timestamp)),
        (
            "weekday",
            F::unary(|arg| {
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(integer_part(arg, "DOW")),
                    Operator::Minus,
                    Box::new(lit(ScalarValue::Int32(Some(1)))),
                ))
            }),
        ),
        ("weekofyear", F::custom(weekofyear)),
        ("window", F::unknown("window")),
        ("window_time", F::unknown("window_time")),
        ("year", F::unary(|arg| integer_part(arg, "YEAR"))),
    ]
}
