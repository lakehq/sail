use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_add_years(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (date, quantity) = values.two()?;
        date_interval(
            context,
            date,
            quantity,
            match name {
                "add_years" => "years",
                "add_months" => "months",
                _ => "days",
            },
            if name == "date_sub" {
                Operator::Minus
            } else {
                Operator::Plus
            },
        )?
    })
}

fn resolve_date_add(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok(match values.len() {
        2 => {
            let (date, quantity) = values.two()?;
            date_interval(context, date, quantity, "days", Operator::Plus)?
        }
        _ => {
            let (unit, quantity, timestamp) = values.three()?;
            let unit = datetime_unit(&unit)?;
            let interval = timestampadd_interval(context, name, &unit, quantity)?;
            let timestamp = context.cast(
                timestamp,
                DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some(context.config.session_timezone.clone()),
                ),
            )?;
            context.binary(timestamp, Operator::Plus, interval)?
        }
    })
}

fn resolve_timestampadd(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (unit, quantity, timestamp) = values.three()?;
        let unit = datetime_unit(&unit)?;
        let interval = timestampadd_interval(context, name, &unit, quantity)?;
        let timestamp = context.cast(
            timestamp,
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(context.config.session_timezone.clone()),
            ),
        )?;
        context.binary(timestamp, Operator::Plus, interval)?
    })
}

fn resolve_date_diff(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    datetime_difference(context, values)
}

fn resolve_make_date(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        crate::analyzer::typing::coercion::validate_arity(name, values.len(), 3)?;
        if values.iter().any(|v| {
            matches!(
                v.kind,
                Deferred::Resolved(ExprKind::Literal(ScalarValue::Null))
            )
        }) {
            context.literal(ScalarValue::Null)
        } else {
            context.scalar("make_date", values)?
        }
    })
}

fn resolve_months_between(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    months_between(context, values)
}

pub(crate) fn datetime_unit(value: &Expr) -> PlanResult<String> {
    match value.kind.resolved("datetime unit")? {
        ExprKind::Literal(value) => value
            .try_as_str()
            .flatten()
            .map(|s| s.trim().to_uppercase())
            .ok_or_else(|| PlanError::invalid("datetime unit must be a string literal or keyword")),
        _ => Err(PlanError::invalid(
            "datetime unit must be a string literal or keyword",
        )),
    }
}

pub(crate) fn date_interval(
    context: &mut FunctionContextInput<'_>,
    date: Expr,
    quantity: Expr,
    unit: &str,
    operator: Operator,
) -> PlanResult<Expr> {
    let suffix = context.literal(ScalarValue::Utf8(Some(format!(" {unit}"))));
    let interval = context.binary(quantity, Operator::StringConcat, suffix)?;
    let interval = context.cast(
        interval,
        DataType::Interval(if unit == "days" {
            IntervalUnit::DayTime
        } else {
            IntervalUnit::YearMonth
        }),
    )?;
    let date = context.cast(date, DataType::Date32)?;
    context.binary(date, operator, interval)
}

pub(crate) fn timestampadd_interval(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    unit: &str,
    quantity: Expr,
) -> PlanResult<Expr> {
    let (index, factor, calendar) = match unit {
        "YEAR" => (0, 1.0, true),
        "QUARTER" => (1, 3.0, true),
        "MONTH" => (1, 1.0, true),
        "WEEK" => (0, 7.0, false),
        "DAY" | "DAYOFYEAR" => (0, 1.0, false),
        "HOUR" => (1, 1.0, false),
        "MINUTE" => (2, 1.0, false),
        "SECOND" => (3, 1.0, false),
        "MILLISECOND" => (3, 0.001, false),
        "MICROSECOND" => (3, 0.000001, false),
        _ => {
            return Err(PlanError::invalid(format!(
                "{name} does not support interval unit type '{unit}'"
            )));
        }
    };
    let quantity = if index == 3 {
        let quantity = context.cast(quantity, DataType::Float64)?;
        let scale = context.literal(ScalarValue::Float64(Some(1.0 / factor)));
        context.binary(quantity, Operator::Divide, scale)?
    } else if factor != 1.0 {
        let scale = context.literal(ScalarValue::Int32(Some(factor as i32)));
        let quantity = context.binary(quantity, Operator::Multiply, scale)?;
        context.cast(quantity, DataType::Int32)?
    } else {
        context.cast(quantity, DataType::Int32)?
    };
    let mut args = (0..if calendar { index + 1 } else { 4 })
        .map(|i| {
            context.literal(if i == 3 {
                ScalarValue::Float64(Some(0.0))
            } else {
                ScalarValue::Int32(Some(0))
            })
        })
        .collect::<Vec<_>>();
    args[index] = quantity;
    let function = if calendar {
        ScalarUDF::from(SparkMakeInterval::new())
    } else {
        ScalarUDF::from(SparkMakeDtInterval::new())
    };
    context.scalar_udf(Arc::new(function), args)
}

pub(crate) fn datetime_difference(
    context: &mut FunctionContextInput<'_>,
    values: Vec<Expr>,
) -> PlanResult<Expr> {
    if values.len() == 2 {
        let (left, right) = values.two()?;
        return date_days_difference(context, left, right);
    }
    let (unit, start, end) = values.three()?;
    let unit = datetime_unit(&unit)?;
    match unit.as_str() {
        "DAY" => date_days_difference(context, end, start),
        "MONTH" | "QUARTER" | "YEAR" => {
            let months = timestamp_month_diff(context, start, end)?;
            let divisor = context.literal(ScalarValue::Int64(Some(match unit.as_str() {
                "YEAR" => 12,
                "QUARTER" => 3,
                _ => 1,
            })));
            context.binary(months, Operator::Divide, divisor)
        }
        "HOUR" | "MINUTE" | "SECOND" | "WEEK" => {
            let start = timestamp_micros(context, start)?;
            let end = timestamp_micros(context, end)?;
            let diff = context.binary(end, Operator::Minus, start)?;
            let divisor = context.literal(ScalarValue::Int64(Some(match unit.as_str() {
                "WEEK" => 604_800_000_000,
                "HOUR" => 3_600_000_000,
                "MINUTE" => 60_000_000,
                _ => 1_000_000,
            })));
            context.binary(diff, Operator::Divide, divisor)
        }
        _ => Err(PlanError::unsupported(format!("datediff unit: {unit}"))),
    }
}

pub(crate) fn date_days_difference(
    context: &mut FunctionContextInput<'_>,
    left: Expr,
    right: Expr,
) -> PlanResult<Expr> {
    let left = context.cast(left, DataType::Date32)?;
    let right = context.cast(right, DataType::Date32)?;
    let left = context.cast(left, DataType::Int64)?;
    let right = context.cast(right, DataType::Int64)?;
    context.binary(left, Operator::Minus, right)
}

pub(crate) fn timestamp_micros(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    let value = context.cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))?;
    context.cast(value, DataType::Int64)
}

pub(crate) fn timestamp_months(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    let year_value = context.copy_expression(&value)?;
    let year = crate::function::scalar::datetime::integer_part(context, year_value, "YEAR")?;
    let month = crate::function::scalar::datetime::integer_part(context, value, "MONTH")?;
    let twelve = context.literal(ScalarValue::Int32(Some(12)));
    let year = context.binary(year, Operator::Multiply, twelve)?;
    context.binary(year, Operator::Plus, month)
}

pub(crate) fn timestamp_time_micros(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    let copy = context.copy_expression(&value)?;
    let day = context.literal(ScalarValue::Utf8(Some("DAY".into())));
    let midnight = context.scalar("date_trunc", vec![day, value])?;
    let midnight = timestamp_micros(context, midnight)?;
    let value = timestamp_micros(context, copy)?;
    context.binary(value, Operator::Minus, midnight)
}

pub(crate) fn timestamp_day_time_before(
    context: &mut FunctionContextInput<'_>,
    left: Expr,
    right: Expr,
) -> PlanResult<Expr> {
    let l = context.copy_expression(&left)?;
    let r = context.copy_expression(&right)?;
    let l = crate::function::scalar::datetime::integer_part(context, l, "DAY")?;
    let r = crate::function::scalar::datetime::integer_part(context, r, "DAY")?;
    let lc = context.copy_expression(&l)?;
    let rc = context.copy_expression(&r)?;
    let before = context.binary(l, Operator::Lt, r)?;
    let same = context.binary(lc, Operator::Eq, rc)?;
    let left = timestamp_time_micros(context, left)?;
    let right = timestamp_time_micros(context, right)?;
    let time_before = context.binary(left, Operator::Lt, right)?;
    let same_before = context.binary(same, Operator::And, time_before)?;
    context.binary(before, Operator::Or, same_before)
}

pub(crate) fn timestamp_month_diff(
    context: &mut FunctionContextInput<'_>,
    start: Expr,
    end: Expr,
) -> PlanResult<Expr> {
    let start = context.cast(start, DataType::Timestamp(TimeUnit::Microsecond, None))?;
    let end = context.cast(end, DataType::Timestamp(TimeUnit::Microsecond, None))?;
    let s = context.copy_expression(&start)?;
    let e = context.copy_expression(&end)?;
    let start_months = timestamp_months(context, s)?;
    let end_months = timestamp_months(context, e)?;
    let months = context.binary(end_months, Operator::Minus, start_months)?;
    let months = context.cast(months, DataType::Int64)?;
    let mut branches = vec![];
    for positive in [true, false] {
        let m = context.copy_expression(&months)?;
        let zero = context.literal(ScalarValue::Int64(Some(0)));
        let condition =
            context.binary(m, if positive { Operator::Gt } else { Operator::Lt }, zero)?;
        let s = context.copy_expression(&start)?;
        let e = context.copy_expression(&end)?;
        let before = if positive {
            timestamp_day_time_before(context, e, s)?
        } else {
            timestamp_day_time_before(context, s, e)?
        };
        let condition = context.binary(condition, Operator::And, before)?;
        let m = context.copy_expression(&months)?;
        let one = context.literal(ScalarValue::Int64(Some(1)));
        let value = context.binary(
            m,
            if positive {
                Operator::Minus
            } else {
                Operator::Plus
            },
            one,
        )?;
        branches.push((condition, value));
    }
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    branches.push((yes, months));
    context.case(branches, None)
}

pub(crate) fn months_between(
    context: &mut FunctionContextInput<'_>,
    mut args: Vec<Expr>,
) -> PlanResult<Expr> {
    let round = if args.len() == 3 {
        args.pop()
            .ok_or_else(|| PlanError::internal("missing round flag"))?
    } else {
        context.literal(ScalarValue::Boolean(Some(true)))
    };
    let (left, right) = args.two()?;
    let mut dates = vec![];
    for value in [left, right] {
        let value = if matches!(
            value.result_type.resolved("months_between date")?.data_type,
            DataType::Timestamp(..)
        ) {
            value
        } else {
            context.cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))?
        };
        dates.push(value);
    }
    let (left, right) = dates.two()?;
    let l = context.copy_expression(&left)?;
    let r = context.copy_expression(&right)?;
    let lm = timestamp_months(context, l)?;
    let rm = timestamp_months(context, r)?;
    let months = context.binary(lm, Operator::Minus, rm)?;
    let months = context.cast(months, DataType::Float64)?;
    let l = context.copy_expression(&left)?;
    let r = context.copy_expression(&right)?;
    let ld = crate::function::scalar::datetime::integer_part(context, l, "DAY")?;
    let rd = crate::function::scalar::datetime::integer_part(context, r, "DAY")?;
    let lc = context.copy_expression(&ld)?;
    let rc = context.copy_expression(&rd)?;
    let same_day = context.binary(lc, Operator::Eq, rc)?;
    let mut last_days = vec![];
    let mut seconds = vec![];
    for date in [left, right] {
        let copy = context.copy_expression(&date)?;
        let day = context.cast(copy, DataType::Date32)?;
        let copy = context.copy_expression(&day)?;
        let last =
            context.scalar_udf(Arc::new(ScalarUDF::from(SparkLastDay::new())), vec![copy])?;
        last_days.push(context.binary(last, Operator::Eq, day)?);
        let micros = timestamp_time_micros(context, date)?;
        let divisor = context.literal(ScalarValue::Int64(Some(1_000_000)));
        seconds.push(context.binary(micros, Operator::Divide, divisor)?);
    }
    let (ll, rl) = last_days.two()?;
    let both_last = context.binary(ll, Operator::And, rl)?;
    let condition = context.binary(same_day, Operator::Or, both_last)?;
    let days_diff = context.binary(ld, Operator::Minus, rd)?;
    let seconds_per_day = context.literal(ScalarValue::Int64(Some(86400)));
    let diff = context.binary(days_diff, Operator::Multiply, seconds_per_day)?;
    let (ls, rs) = seconds.two()?;
    let diff = context.binary(diff, Operator::Plus, ls)?;
    let diff = context.binary(diff, Operator::Minus, rs)?;
    let divisor = context.literal(ScalarValue::Float64(Some(2678400.0)));
    let fraction = context.binary(diff, Operator::Divide, divisor)?;
    let copy = context.copy_expression(&months)?;
    let fractional = context.binary(copy, Operator::Plus, fraction)?;
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    let result = context.case(vec![(condition, months), (yes, fractional)], None)?;
    let copy = context.copy_expression(&result)?;
    let precision = context.literal(ScalarValue::Int32(Some(8)));
    let rounded = context.scalar("round", vec![copy, precision])?;
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    context.case(vec![(round, rounded), (yes, result)], None)
}

pub(in crate::function::scalar) fn list_built_in_datetime_arithmetic_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "add_years",
            F::custom(|input| resolve_add_years(input, "add_years")),
        ),
        (
            "add_months",
            F::custom(|input| resolve_add_years(input, "add_months")),
        ),
        (
            "add_days",
            F::custom(|input| resolve_add_years(input, "add_days")),
        ),
        (
            "date_add",
            F::custom(|input| resolve_date_add(input, "date_add")),
        ),
        (
            "dateadd",
            F::custom(|input| resolve_date_add(input, "dateadd")),
        ),
        (
            "date_sub",
            F::custom(|input| resolve_add_years(input, "date_sub")),
        ),
        (
            "timestampadd",
            F::custom(|input| resolve_timestampadd(input, "timestampadd")),
        ),
        (
            "timestamp_add",
            F::custom(|input| resolve_timestampadd(input, "timestamp_add")),
        ),
        ("date_diff", F::custom(resolve_date_diff)),
        ("datediff", F::custom(resolve_date_diff)),
        ("timestampdiff", F::custom(resolve_date_diff)),
        ("timestamp_diff", F::custom(resolve_date_diff)),
        (
            "make_date",
            F::custom(|input| resolve_make_date(input, "make_date")),
        ),
        ("months_between", F::custom(resolve_months_between)),
    ]
}
