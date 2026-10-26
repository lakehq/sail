use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF};
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common::utils::datetime::MICROS_PER_DAY;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};
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
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

/// The parsed durations (in microseconds) of a Spark `window` call.
#[derive(Debug, Clone, Copy)]
struct WindowSpec {
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

/// Bound on `ceil(windowDuration / slideDuration)`
const MAX_OVERLAPPING_WINDOWS: i64 = 1_000_000;

pub(crate) fn integer_part(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    part: &str,
) -> PlanResult<Expr> {
    let part = context.literal(ScalarValue::Utf8(Some(part.to_uppercase())));
    let result = context.scalar("date_part", vec![part, value])?;
    context.cast(result, DataType::Int32)
}

pub(crate) fn trunc_part_conversion(
    context: &mut FunctionContextInput<'_>,
    part: Expr,
) -> PlanResult<Expr> {
    let mut branches = vec![];
    for (aliases, name) in [
        (vec!["mon", "mm"], "month"),
        (vec!["yy", "yyyy"], "year"),
        (vec!["dd"], "day"),
    ] {
        let mut condition = None;
        for alias in aliases {
            let value = context.copy_expression(&part)?;
            let alias = context.literal(ScalarValue::Utf8(Some(alias.into())));
            let test = crate::function::scalar::predicate::pattern(
                context, value, alias, false, true, None,
            )?;
            condition = Some(match condition {
                Some(previous) => context.binary(previous, Operator::Or, test)?,
                None => test,
            });
        }
        let condition = condition.ok_or_else(|| PlanError::internal("empty trunc aliases"))?;
        let value = context.literal(ScalarValue::Utf8(Some(name.into())));
        branches.push((condition, value));
    }
    context.case(branches, Some(part))
}

fn date_trunc(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let (first, second) = values.two()?;
    let (value, part) = if name == "trunc" {
        (first, second)
    } else {
        (second, first)
    };
    let part = trunc_part_conversion(context, part)?;
    let result = if name == "trunc" {
        context.scalar("date_trunc", vec![part, value])?
    } else {
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkDateTrunc::new())),
            vec![part, value],
        )?
    };
    let target = if name == "trunc" {
        DataType::Date32
    } else {
        match &result.result_type.resolved("date_trunc result")?.data_type {
            DataType::Timestamp(_, timezone) => {
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
            }
            other => {
                return Err(PlanError::internal(format!(
                    "date_trunc expected a timestamp result, got {other:?}"
                )));
            }
        }
    };
    context.cast(result, target)
}

fn interval_arithmetic(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

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
    )
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

fn timestampadd(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

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
    context.binary(timestamp, Operator::Plus, interval)
}

fn dateadd(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
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

fn make_date(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    crate::analyzer::typing::coercion::validate_arity(name, values.len(), 3)?;
    Ok(
        if values.iter().any(|v| {
            matches!(
                v.kind,
                Deferred::Resolved(ExprKind::Literal(ScalarValue::Null))
            )
        }) {
            context.literal(ScalarValue::Null)
        } else {
            context.scalar("make_date", values)?
        },
    )
}

pub(crate) fn timestamp_micros(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    let value = context.cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))?;
    context.cast(value, DataType::Int64)
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

pub(crate) fn timestamp_day_time_is_before(
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
            timestamp_day_time_is_before(context, e, s)?
        } else {
            timestamp_day_time_is_before(context, s, e)?
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

fn datediff(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

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

fn current_timezone(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    values.zero()?;
    Ok(context.literal(ScalarValue::Utf8(Some(timezone.to_string()))))
}

pub(crate) fn declare_nullable_result(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    nullable: bool,
) -> PlanResult<Expr> {
    if nullable && !value.result_type.resolved("nullable result")?.nullable {
        let null = context.literal(ScalarValue::try_from(
            &value.result_type.resolved("nullable result")?.data_type,
        )?);
        let yes = context.literal(ScalarValue::Boolean(Some(true)));
        context.case(vec![(yes, value)], Some(null))
    } else {
        Ok(value)
    }
}

fn to_date(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let null_on_error = !context.config.ansi_mode;
    if values.len() == 1 {
        let value = values.one()?;
        if value
            .result_type
            .resolved("to_date input")?
            .data_type
            .is_string()
        {
            return context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkDate::new(null_on_error))),
                vec![value],
            );
        }
        return context.cast(value, DataType::Date32);
    }
    let (value, format) = values.two()?;
    let field = format.result_type.resolved("date format")?;
    if field.data_type.is_nested() {
        return Err(PlanError::invalid(format!(
            "to_date format argument must be a string, got {}",
            field.data_type
        )));
    }
    let format = if field.data_type.is_string() {
        format
    } else {
        context.cast(format, DataType::Utf8)?
    };
    let source = value.result_type.resolved("to_date input")?;
    let kind = source.data_type.clone();
    if matches!(
        kind,
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
    ) {
        let nullable = null_on_error
            || source.nullable
            || format.result_type.resolved("date format")?.nullable;
        let value = if matches!(kind, DataType::Timestamp(..)) {
            context.scalar("to_local_time", vec![value])?
        } else {
            value
        };
        let value = context.cast(value, DataType::Date32)?;
        return declare_nullable_result(context, value, nullable);
    }
    let value = if kind.is_string() || kind.is_nested() {
        value
    } else {
        context.cast(value, DataType::Utf8)?
    };
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkDate::new(null_on_error))),
        vec![value, format],
    )
}

fn unix_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    let ansi = context.config.ansi_mode;
    if (values.is_empty() && name == "to_unix_timestamp") || values.len() > 2 {
        return Err(PlanError::invalid(format!(
            "{name} requires {} arguments",
            if name == "unix_timestamp" {
                "0, 1, or 2"
            } else {
                "1 or 2"
            }
        )));
    }
    Ok(if values.is_empty() {
        let value = context.scalar_udf(
            Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Second,
            ))),
            vec![],
        )?;
        context.cast(value, DataType::Int64)?
    } else {
        let mut nullable = false;
        if values.len() == 2
            && matches!(
                values[0]
                    .result_type
                    .resolved("unix_timestamp input")?
                    .data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) | DataType::Null
            )
        {
            let format = values
                .pop()
                .ok_or_else(|| PlanError::internal("missing format"))?;
            let field = format.result_type.resolved("unix_timestamp format")?;
            if !(field.data_type.is_string() || field.data_type.is_null()) {
                return Err(PlanError::invalid(format!(
                    "unix_timestamp format argument must be a string, got {}",
                    field.data_type
                )));
            }
            nullable = !ansi
                || values[0]
                    .result_type
                    .resolved("unix_timestamp value")?
                    .nullable
                || field.nullable;
        }
        let value = context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi))),
            values,
        )?;
        declare_nullable_result(context, value, nullable)?
    })
}

fn next_day(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let ansi = context.config.ansi_mode;
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkNextDay::new(ansi))), values)
}

fn date_format(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    crate::analyzer::typing::coercion::validate_arity(name, values.len(), 2)?;
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkDateFormat::new(timezone))),
        values,
    )
}

fn to_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let ntz = name == "to_timestamp_ntz"
        || (name != "to_timestamp_ltz"
            && matches!(
                context.config.default_timestamp_type,
                DefaultTimestampType::TimestampNtz
            ));
    let timezone = if ntz {
        None
    } else {
        Some(context.config.session_timezone.clone())
    };
    let target = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    let safe = name == "try_to_timestamp";
    let ansi = context.config.ansi_mode;
    if values.len() == 1 {
        let value = values.one()?;
        let kind = &value.result_type.resolved("timestamp input")?.data_type;
        if kind.is_string() {
            return context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                    timezone, ansi, safe,
                )?)),
                vec![value],
            );
        }
        let value = if matches!(kind, DataType::Timestamp(_, Some(_))) {
            context.scalar("to_local_time", vec![value])?
        } else {
            value
        };
        return if safe {
            context.try_cast(value, target)
        } else {
            context.cast(value, target)
        };
    }
    if values.len() != 2 {
        return Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )));
    }
    if values
        .iter()
        .any(|v| matches!(&v.kind, Deferred::Resolved(ExprKind::Literal(v)) if v.is_null()))
    {
        return Ok(context.literal(ScalarValue::TimestampMicrosecond(None, timezone)));
    }
    let value = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
            timezone, ansi, safe,
        )?)),
        values,
    )?;
    context.cast(value, target)
}

fn to_time(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    if !matches!(values.len(), 1 | 2) {
        return Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )));
    }
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkTime::new(name == "try_to_time"))),
        values,
    )
}

fn from_unixtime(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    if values.len() == 1 {
        values.push(context.literal(ScalarValue::Utf8(Some("yyyy-MM-dd HH:mm:ss".into()))));
    }
    let (value, format) = values.two()?;
    let value = context.cast(
        value,
        DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
    )?;
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkDateFormat::new(timezone))),
        vec![value, format],
    )
}

pub(crate) fn ntz_timestamp_and_unit(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    relative_to_utc: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    let kind = value
        .result_type
        .resolved("timestamp input")?
        .data_type
        .clone();
    let unit = match kind {
        DataType::Timestamp(unit, _) => unit,
        _ => TimeUnit::Microsecond,
    };
    let timezone = relative_to_utc.then(|| context.config.session_timezone.clone());
    let result = match kind {
        DataType::Timestamp(_, Some(_)) if !relative_to_utc => {
            context.scalar("to_local_time", vec![value])?
        }
        DataType::Timestamp(_, Some(_)) => value,
        DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64 => {
            context.cast(value, DataType::Timestamp(unit, timezone))?
        }
        DataType::Null if !relative_to_utc => {
            context.cast(value, DataType::Timestamp(unit, None))?
        }
        kind if kind.is_string() => context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                timezone,
                context.config.ansi_mode,
                false,
            )?)),
            vec![value],
        )?,
        other => {
            return Err(PlanError::invalid(format!(
                "invalid {}NTZ timestamp type: {other:?}",
                if relative_to_utc { "UTC " } else { "" }
            )));
        }
    };
    let result = if relative_to_utc {
        context.cast(result, DataType::Timestamp(unit, None))?
    } else {
        result
    };
    Ok((result, unit))
}

fn convert_timezone(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    if values.len() == 2 {
        values.insert(
            0,
            context.literal(ScalarValue::Utf8(Some(session_tz.to_string()))),
        );
    }
    let (from, to, timestamp) = values.three()?;
    for value in [&from, &to] {
        let field = value.result_type.resolved("time zone")?.to_field("");
        if !datafusion::arrow::compute::can_cast_types(field.data_type(), &DataType::Utf8)
            && !is_variant_storage_field(field.as_ref())
        {
            return Err(PlanError::invalid(format!(
                "convert_timezone time zone arguments must be castable to string, got {}",
                field.data_type()
            )));
        }
    }
    let (timestamp, _) = ntz_timestamp_and_unit(context, timestamp, false)?;
    let args = vec![from, to, timestamp];
    let mut eager = false;
    for arg in &args {
        eager |= arg.any(&|node| match node {
            ExprKind::ScalarFunction { function, .. } => {
                function
                    .inner()
                    .is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>()
                    || function.inner().is::<SparkCastToVariant>()
            }
            _ => false,
        })?;
    }
    let function = ConvertTz::new(true).with_null_short_circuit();
    Ok(if eager {
        context.scalar_udf(Arc::new(ScalarUDF::from(function)), args)?
    } else {
        crate::function::scalar::lambda::lazy_arguments(
            context,
            Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(function))),
            args,
        )?
    })
}

fn from_utc_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    let (timestamp, zone) = values.two()?;
    let (timestamp, unit) = ntz_timestamp_and_unit(context, timestamp, true)?;
    let utc = context.literal(ScalarValue::Utf8(Some("UTC".into())));
    let args = if name == "from_utc_timestamp" {
        vec![utc, zone, timestamp]
    } else {
        vec![zone, utc, timestamp]
    };
    let result = context.scalar_udf(Arc::new(ScalarUDF::from(ConvertTz::new(false))), args)?;
    let result = context.cast(result, DataType::Timestamp(unit, Some("UTC".into())))?;
    context.cast(result, DataType::Timestamp(unit, Some(session_tz)))
}

fn make_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    let safe = name.starts_with("try_");
    let default = matches!(name, "make_timestamp" | "try_make_timestamp");
    let ntz = name.ends_with("_ntz")
        || (default
            && matches!(
                context.config.default_timestamp_type,
                DefaultTimestampType::TimestampNtz
            ));
    if default && values.len() == 1 {
        values.push(context.literal(ScalarValue::Time64Microsecond(Some(0))));
    }
    let zone = if matches!(values.len(), 3 | 7) && (!ntz || default) {
        values.pop()
    } else {
        None
    };
    if !matches!(values.len(), 2 | 6) {
        return Err(PlanError::invalid(format!(
            "{name} requires {} arguments",
            if ntz { "2 or 6" } else { "2, 3, 6 or 7" }
        )));
    }
    let mut result = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new(safe))),
        values,
    )?;
    if !ntz {
        if let Some(zone) = zone {
            let target = context.literal(ScalarValue::Utf8(Some(session_tz.to_string())));
            result = context.scalar_udf(
                Arc::new(ScalarUDF::from(ConvertTz::new(true))),
                vec![zone, target, result],
            )?;
        }
        result = context.cast(
            result,
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz)),
        )?;
    }
    Ok(result)
}

fn date_part(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (part, date) = values.two()?;
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkDatePart::new())),
        vec![part, date],
    )
}

fn months_between(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut args = input.arguments;

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

fn window_interval_micros(expr: &Expr) -> PlanResult<i64> {
    let Deferred::Resolved(ExprKind::Literal(value)) = &expr.kind else {
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

fn window_time(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let kind = &value
        .result_type
        .resolved("window_time argument")?
        .data_type;
    let target = match kind {
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "start"
                && fields[1].name() == "end"
                && fields.iter().all(|f| {
                    matches!(f.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _))
                }) =>
        {
            fields[1].data_type().clone()
        }
        _ => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {kind:?}"
            )));
        }
    };
    let key = context.literal(ScalarValue::Utf8(Some("end".into())));
    let end = context.scalar("get_field", vec![value, key])?;
    let end = context.cast(end, DataType::Int64)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let result = context.binary(end, Operator::Minus, one)?;
    context.cast(result, target)
}

fn resolve_curdate(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    values.zero()?;
    context.scalar("current_date", vec![])
}

fn resolve_current_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    values.zero()?;
    let result = context.scalar_udf(
        Arc::new(ScalarUDF::from(TimestampNow::new(
            timezone,
            TimeUnit::Microsecond,
        ))),
        vec![],
    )?;
    Ok(if name == "localtimestamp" {
        context.scalar("to_local_time", vec![result])?
    } else {
        result
    })
}

fn resolve_dayname(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let pattern = context.literal(ScalarValue::Utf8(Some(
        match name {
            "dayname" => "%a",
            "monthname" => "%b",
            _ => "%V",
        }
        .into(),
    )));
    let result = context.scalar("to_char", vec![value, pattern])?;
    Ok(if name == "weekofyear" {
        context.cast(result, DataType::Int32)?
    } else {
        result
    })
}

fn resolve_date_from_unix_date(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.cast(values.one()?, DataType::Date32)
}

fn resolve_unix_date(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = context.cast(values.one()?, DataType::Date32)?;
    context.cast(value, DataType::Int32)
}

fn resolve_unix_micros(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    let unit = match name {
        "unix_micros" => TimeUnit::Microsecond,
        "unix_millis" => TimeUnit::Millisecond,
        _ => TimeUnit::Second,
    };
    let value = context.cast(values.one()?, DataType::Timestamp(unit, Some(timezone)))?;
    context.cast(value, DataType::Int64)
}

fn resolve_timestamp_micros(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let mut value = values.one()?;
    if name != "timestamp_micros" {
        value = context.cast(value, DataType::Int64)?;
        let multiplier = context.literal(ScalarValue::Int64(Some(if name == "timestamp_millis" {
            1000
        } else {
            1_000_000
        })));
        value = context.binary(value, Operator::Multiply, multiplier)?;
    }
    context.cast(
        value,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
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

pub(crate) fn window_buckets(
    context: &mut FunctionContextInput<'_>,
    values: Vec<Expr>,
) -> PlanResult<Expr> {
    let spec = parse_window_spec(&values)?;
    let time = values
        .into_iter()
        .next()
        .ok_or_else(|| PlanError::internal("window missing time column"))?;
    let target = window_field_type(
        &time.result_type.resolved("window time")?.data_type,
        &context.config.session_timezone,
    )?;
    let time = context.cast(time, target)?;
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkWindowBuckets::new(
            spec.window_duration,
            spec.slide_duration,
            spec.start_time,
        ))),
        vec![time],
    )
}

pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|input| interval_arithmetic(input, "add_years")),
        ),
        (
            "add_months",
            F::custom(|input| interval_arithmetic(input, "add_months")),
        ),
        (
            "add_days",
            F::custom(|input| interval_arithmetic(input, "add_days")),
        ),
        ("convert_timezone", F::custom(convert_timezone)),
        ("curdate", F::custom(resolve_curdate)),
        ("current_date", F::kernel("current_date")),
        ("current_time", F::kernel("current_time")),
        (
            "current_timestamp",
            F::custom(|input| resolve_current_timestamp(input, "current_timestamp")),
        ),
        ("current_timezone", F::custom(current_timezone)),
        ("date_add", F::custom(|input| dateadd(input, "date_add"))),
        ("date_diff", F::custom(datediff)),
        (
            "date_format",
            F::custom(|input| date_format(input, "date_format")),
        ),
        (
            "date_from_unix_date",
            F::custom(resolve_date_from_unix_date),
        ),
        ("date_part", F::custom(date_part)),
        (
            "date_sub",
            F::custom(|input| interval_arithmetic(input, "date_sub")),
        ),
        (
            "date_trunc",
            F::custom(|input| date_trunc(input, "date_trunc")),
        ),
        ("dateadd", F::custom(|input| dateadd(input, "dateadd"))),
        ("datediff", F::custom(datediff)),
        ("datepart", F::custom(date_part)),
        (
            "day",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DAY")?;
                Ok(result)
            }),
        ),
        (
            "dayname",
            F::custom(|input| resolve_dayname(input, "dayname")),
        ),
        (
            "dayofmonth",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DAY")?;
                Ok(result)
            }),
        ),
        (
            "dayofweek",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DOW")?;
                let one = input.context.literal(ScalarValue::Int32(Some(1)));
                input.context.binary(result, Operator::Plus, one)
            }),
        ),
        (
            "dayofyear",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DOY")?;
                Ok(result)
            }),
        ),
        ("extract", F::custom(date_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        (
            "from_utc_timestamp",
            F::custom(|input| from_utc_timestamp(input, "from_utc_timestamp")),
        ),
        (
            "hour",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "HOUR")?;
                Ok(result)
            }),
        ),
        ("last_day", F::udf(SparkLastDay::new())),
        (
            "localtimestamp",
            F::custom(|input| resolve_current_timestamp(input, "localtimestamp")),
        ),
        (
            "make_date",
            F::custom(|input| make_date(input, "make_date")),
        ),
        ("make_dt_interval", F::udf(SparkMakeDtInterval::new())),
        ("make_interval", F::udf(SparkMakeInterval::new())),
        ("make_time", F::udf(SparkMakeTime::new())),
        (
            "make_timestamp",
            F::custom(|input| make_timestamp(input, "make_timestamp")),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| make_timestamp(input, "make_timestamp_ltz")),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| make_timestamp(input, "make_timestamp_ntz")),
        ),
        ("make_ym_interval", F::udf(SparkMakeYmInterval::new())),
        (
            "minute",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "MINUTE")?;
                Ok(result)
            }),
        ),
        (
            "month",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "MONTH")?;
                Ok(result)
            }),
        ),
        (
            "monthname",
            F::custom(|input| resolve_dayname(input, "monthname")),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        (
            "now",
            F::custom(|input| resolve_current_timestamp(input, "now")),
        ),
        (
            "quarter",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "QUARTER")?;
                Ok(result)
            }),
        ),
        (
            "second",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "SECOND")?;
                Ok(result)
            }),
        ),
        ("session_window", F::kernel("session_window")),
        ("time_bucket", F::kernel("time_bucket")),
        ("time_from_micros", F::kernel("time_from_micros")),
        ("time_from_millis", F::kernel("time_from_millis")),
        ("time_from_seconds", F::kernel("time_from_seconds")),
        ("time_to_micros", F::kernel("time_to_micros")),
        ("time_to_millis", F::kernel("time_to_millis")),
        ("time_to_seconds", F::kernel("time_to_seconds")),
        (
            "timestamp_micros",
            F::custom(|input| resolve_timestamp_micros(input, "timestamp_micros")),
        ),
        (
            "timestamp_millis",
            F::custom(|input| resolve_timestamp_micros(input, "timestamp_millis")),
        ),
        (
            "timestamp_seconds",
            F::custom(|input| resolve_timestamp_micros(input, "timestamp_seconds")),
        ),
        (
            "timestampadd",
            F::custom(|input| timestampadd(input, "timestampadd")),
        ),
        (
            "timestamp_add",
            F::custom(|input| timestampadd(input, "timestamp_add")),
        ),
        ("timestampdiff", F::custom(datediff)),
        ("timestamp_diff", F::custom(datediff)),
        ("to_date", F::custom(to_date)),
        ("to_time", F::custom(|input| to_time(input, "to_time"))),
        (
            "try_to_time",
            F::custom(|input| to_time(input, "try_to_time")),
        ),
        (
            "to_timestamp",
            F::custom(|input| to_timestamp(input, "to_timestamp")),
        ),
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, "to_timestamp_ltz")),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, "to_timestamp_ntz")),
        ),
        (
            "to_unix_timestamp",
            F::custom(|input| unix_timestamp(input, "to_unix_timestamp")),
        ),
        (
            "to_utc_timestamp",
            F::custom(|input| from_utc_timestamp(input, "to_utc_timestamp")),
        ),
        ("trunc", F::custom(|input| date_trunc(input, "trunc"))),
        ("try_make_interval", F::udf(SparkMakeInterval::new())),
        (
            "try_make_timestamp",
            F::custom(|input| make_timestamp(input, "try_make_timestamp")),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| make_timestamp(input, "try_make_timestamp_ltz")),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| make_timestamp(input, "try_make_timestamp_ntz")),
        ),
        (
            "try_to_timestamp",
            F::custom(|input| to_timestamp(input, "try_to_timestamp")),
        ),
        ("time_diff", F::udf(SparkTimeDiff::new())),
        ("time_trunc", F::udf(SparkTimeTrunc::new())),
        ("unix_date", F::custom(resolve_unix_date)),
        (
            "unix_micros",
            F::custom(|input| resolve_unix_micros(input, "unix_micros")),
        ),
        (
            "unix_millis",
            F::custom(|input| resolve_unix_micros(input, "unix_millis")),
        ),
        (
            "unix_seconds",
            F::custom(|input| resolve_unix_micros(input, "unix_seconds")),
        ),
        (
            "unix_timestamp",
            F::custom(|input| unix_timestamp(input, "unix_timestamp")),
        ),
        (
            "weekday",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DOW")?;
                let one = input.context.literal(ScalarValue::Int32(Some(1)));
                input.context.binary(result, Operator::Minus, one)
            }),
        ),
        (
            "weekofyear",
            F::custom(|input| resolve_dayname(input, "weekofyear")),
        ),
        ("window", F::kernel("window")),
        ("window_time", F::custom(window_time)),
        ("year", F::udf(SparkYear::new())),
        (
            "years",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "YEAR")?;
                Ok(result)
            }),
        ),
    ]
}
