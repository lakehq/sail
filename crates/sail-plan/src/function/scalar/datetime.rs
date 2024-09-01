use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType,
};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::{lit, BinaryExpr, Operator};

use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;
use crate::utils::ItemTaker;

fn integer_part(expr: Expr, part: String) -> Expr {
    let part = lit(ScalarValue::Utf8(Some(part.to_uppercase())));
    Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::date_part(part, expr)),
        data_type: DataType::Int32,
    })
}

fn trunc(args: Vec<Expr>) -> PlanResult<Expr> {
    let (date, part) = args.two()?;
    Ok(expr_fn::date_trunc(part, date))
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
                "add_interval does not support interval unit type '{}'",
                unit
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

// FIXME: Spark displays dates and timestamps according to the session time zone.
//  We should be setting the DataFusion config `datafusion.execution.time_zone`
//  and casting any datetime functions that don't use the DataFusion config.
pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|args| interval_arithmetic(args, "years", Operator::Plus)),
        ),
        (
            "add_months",
            F::custom(|args| interval_arithmetic(args, "months", Operator::Plus)),
        ),
        (
            "add_days",
            F::custom(|args| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        ("convert_timezone", F::unknown("convert_timezone")),
        ("curdate", F::nullary(expr_fn::current_date)),
        ("current_date", F::nullary(expr_fn::current_date)),
        ("current_timestamp", F::nullary(expr_fn::now)),
        ("current_timezone", F::unknown("current_timezone")),
        (
            "date_add",
            F::custom(|args| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        ("date_diff", F::unknown("date_diff")),
        ("date_format", F::unknown("date_format")),
        ("date_from_unix_date", F::unknown("date_from_unix_date")),
        ("date_part", F::binary(expr_fn::date_part)),
        (
            "date_sub",
            F::custom(|args| interval_arithmetic(args, "days", Operator::Minus)),
        ),
        ("date_trunc", F::binary(expr_fn::date_trunc)),
        (
            "dateadd",
            F::custom(|args| interval_arithmetic(args, "days", Operator::Plus)),
        ),
        ("datediff", F::unknown("datediff")),
        ("datepart", F::binary(expr_fn::date_part)),
        ("day", F::unary(|x| integer_part(x, "DAY".to_string()))),
        ("dayofmonth", F::unknown("dayofmonth")),
        ("dayofweek", F::unknown("dayofweek")),
        ("dayofyear", F::unknown("dayofyear")),
        ("extract", F::binary(expr_fn::date_part)),
        ("from_unixtime", F::unknown("from_unixtime")),
        ("from_utc_timestamp", F::unknown("from_utc_timestamp")),
        ("hour", F::unknown("hour")),
        ("last_day", F::unknown("last_day")),
        ("localtimestamp", F::unknown("localtimestamp")),
        ("make_date", F::unknown("make_date")),
        ("make_dt_interval", F::unknown("make_dt_interval")),
        ("make_interval", F::unknown("make_interval")),
        ("make_timestamp", F::unknown("make_timestamp")),
        ("make_timestamp_ltz", F::unknown("make_timestamp_ltz")),
        ("make_timestamp_ntz", F::unknown("make_timestamp_ntz")),
        ("make_ym_interval", F::unknown("make_ym_interval")),
        ("minute", F::unknown("minute")),
        ("month", F::unary(|x| integer_part(x, "MONTH".to_string()))),
        ("months_between", F::unknown("months_between")),
        ("next_day", F::unknown("next_day")),
        ("now", F::nullary(expr_fn::now)),
        ("quarter", F::unknown("quarter")),
        ("second", F::unknown("second")),
        ("session_window", F::unknown("session_window")),
        ("timestamp_micros", F::unknown("timestamp_micros")),
        ("timestamp_millis", F::unknown("timestamp_millis")),
        ("timestamp_seconds", F::unknown("timestamp_seconds")),
        ("to_date", F::unknown("to_date")),
        ("to_timestamp", F::var_arg(expr_fn::to_timestamp_micros)),
        ("to_timestamp_ltz", F::unknown("to_timestamp_ltz")),
        ("to_timestamp_ntz", F::unknown("to_timestamp_ntz")),
        ("to_unix_timestamp", F::unknown("to_unix_timestamp")),
        ("to_utc_timestamp", F::unknown("to_utc_timestamp")),
        ("trunc", F::custom(trunc)),
        ("try_to_timestamp", F::unknown("try_to_timestamp")),
        ("unix_date", F::unknown("unix_date")),
        ("unix_micros", F::unknown("unix_micros")),
        ("unix_millis", F::unknown("unix_millis")),
        ("unix_seconds", F::unknown("unix_seconds")),
        ("unix_timestamp", F::var_arg(expr_fn::to_unixtime)),
        ("weekday", F::unknown("weekday")),
        ("weekofyear", F::unknown("weekofyear")),
        ("window", F::unknown("window")),
        ("window_time", F::unknown("window_time")),
        ("year", F::unary(|x| integer_part(x, "YEAR".to_string()))),
    ]
}
