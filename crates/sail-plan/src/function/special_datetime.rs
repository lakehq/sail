use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalDayTimeType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator, ScalarUDF, cast, lit};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::datetime::parse_spark_timezone;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;

#[derive(Clone, Copy)]
enum SpecialDatetime {
    Epoch,
    Now,
    Today,
    Tomorrow,
    Yesterday,
}

fn special_datetime(expr: &Expr) -> Option<SpecialDatetime> {
    if expr.any_column_refs() || expr.contains_outer() || expr.is_volatile() {
        return None;
    }

    let value = match LiteralEvaluator::new().evaluate(expr).ok()? {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => value,
        _ => return None,
    };
    let value = value.trim_matches(|character| character <= '\u{20}');
    let name_end = value
        .char_indices()
        .find_map(|(index, character)| (!character.is_alphabetic()).then_some(index))
        .unwrap_or(value.len());
    if name_end < 3 {
        return None;
    }

    let name = &value[..name_end];
    let timezone = value[name_end..].trim_start_matches([' ', '\t']);
    if (!timezone.is_empty() && parse_spark_timezone(timezone).is_err())
        || (name.eq_ignore_ascii_case("now") && !timezone.is_empty())
    {
        return None;
    }

    if name.eq_ignore_ascii_case("epoch") {
        Some(SpecialDatetime::Epoch)
    } else if name.eq_ignore_ascii_case("now") {
        Some(SpecialDatetime::Now)
    } else if name.eq_ignore_ascii_case("today") {
        Some(SpecialDatetime::Today)
    } else if name.eq_ignore_ascii_case("tomorrow") {
        Some(SpecialDatetime::Tomorrow)
    } else if name.eq_ignore_ascii_case("yesterday") {
        Some(SpecialDatetime::Yesterday)
    } else {
        None
    }
}

fn relative_current_date(days: i32) -> Expr {
    let date = expr_fn::current_date();
    if days == 0 {
        date
    } else {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(date),
            Operator::Plus,
            Box::new(lit(ScalarValue::IntervalDayTime(Some(
                IntervalDayTimeType::make_value(days, 0),
            )))),
        ))
    }
}

fn current_timestamp(session_timezone: &Arc<str>) -> Expr {
    ScalarUDF::from(TimestampNow::new(
        Arc::clone(session_timezone),
        TimeUnit::Microsecond,
    ))
    .call(vec![])
}

fn date_to_timestamp(date: Expr, target_type: &DataType, session_timezone: &Arc<str>) -> Expr {
    match target_type {
        DataType::Timestamp(TimeUnit::Microsecond, None) => cast(date, target_type.clone()),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let timestamp_ntz = cast(date, DataType::Timestamp(TimeUnit::Microsecond, None));
            let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
                lit(session_timezone.to_string()),
                lit("UTC"),
                timestamp_ntz,
            ]);
            cast(cast(instant, DataType::Int64), target_type.clone())
        }
        _ => unreachable!(),
    }
}

pub(crate) fn foldable_special_datetime_cast(
    expr: &Expr,
    target_type: &DataType,
    session_timezone: &Arc<str>,
) -> Option<Expr> {
    let special = special_datetime(expr)?;
    match target_type {
        DataType::Date32 => Some(match special {
            SpecialDatetime::Epoch => lit(ScalarValue::Date32(Some(0))),
            SpecialDatetime::Now | SpecialDatetime::Today => relative_current_date(0),
            SpecialDatetime::Tomorrow => relative_current_date(1),
            SpecialDatetime::Yesterday => relative_current_date(-1),
        }),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Some(match special {
            SpecialDatetime::Epoch => {
                lit(ScalarValue::TimestampMicrosecond(Some(0), timezone.clone()))
            }
            SpecialDatetime::Now => {
                let now = current_timestamp(session_timezone);
                if timezone.is_some() {
                    cast(cast(now, DataType::Int64), target_type.clone())
                } else {
                    expr_fn::to_local_time(vec![now])
                }
            }
            SpecialDatetime::Today => {
                date_to_timestamp(relative_current_date(0), target_type, session_timezone)
            }
            SpecialDatetime::Tomorrow => {
                date_to_timestamp(relative_current_date(1), target_type, session_timezone)
            }
            SpecialDatetime::Yesterday => {
                date_to_timestamp(relative_current_date(-1), target_type, session_timezone)
            }
        }),
        _ => None,
    }
}
