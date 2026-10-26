use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_curdate(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        values.zero()?;
        context.scalar("current_date", vec![])?
    })
}

fn resolve_window_time(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    time_window::window_time(context, values)
}

fn resolve_current_timezone(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        values.zero()?;
        context.literal(ScalarValue::Utf8(Some(timezone.to_string())))
    })
}

fn resolve_current_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        values.zero()?;
        let result = context.scalar_udf(
            Arc::new(ScalarUDF::from(TimestampNow::new(
                timezone,
                TimeUnit::Microsecond,
            ))),
            vec![],
        )?;
        if name == "localtimestamp" {
            context.scalar("to_local_time", vec![result])?
        } else {
            result
        }
    })
}

fn resolve_date_format(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        crate::analyzer::typing::coercion::validate_arity(name, values.len(), 2)?;
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkDateFormat::new(timezone))),
            values,
        )?
    })
}

fn resolve_date_part(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (part, date) = values.two()?;
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkDatePart::new())),
            vec![part, date],
        )?
    })
}

fn resolve_dayname(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
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
        if name == "weekofyear" {
            context.cast(result, DataType::Int32)?
        } else {
            result
        }
    })
}

fn resolve_date_trunc(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    Ok({
        let (first, second) = values.two()?;
        let (value, part) = if name == "trunc" {
            (first, second)
        } else {
            (second, first)
        };
        let part = trunc_part(context, part)?;
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
        context.cast(result, target)?
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

    Ok({
        let value = context.cast(values.one()?, DataType::Date32)?;
        context.cast(value, DataType::Int32)?
    })
}

fn resolve_unix_micros(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        let unit = match name {
            "unix_micros" => TimeUnit::Microsecond,
            "unix_millis" => TimeUnit::Millisecond,
            _ => TimeUnit::Second,
        };
        let value = context.cast(values.one()?, DataType::Timestamp(unit, Some(timezone)))?;
        context.cast(value, DataType::Int64)?
    })
}

fn resolve_timestamp_micros(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let mut value = values.one()?;
        if name != "timestamp_micros" {
            value = context.cast(value, DataType::Int64)?;
            let multiplier =
                context.literal(ScalarValue::Int64(Some(if name == "timestamp_millis" {
                    1000
                } else {
                    1_000_000
                })));
            value = context.binary(value, Operator::Multiply, multiplier)?;
        }
        context.cast(
            value,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )?
    })
}

fn resolve_next_day(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let ansi = context.config.ansi_mode;
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkNextDay::new(ansi))), values)
}

fn resolve_to_time(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        if !matches!(values.len(), 1 | 2) {
            return Err(PlanError::invalid(format!(
                "{name} requires 1 or 2 arguments"
            )));
        }
        context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkTime::new(name == "try_to_time"))),
            values,
        )?
    })
}

fn to_date(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    resolve_to_date(context, values)
}

fn to_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    resolve_to_timestamp(context, name, values)
}

fn resolve_unix_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    let ansi = context.config.ansi_mode;
    Ok({
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
        if values.is_empty() {
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
            declare_nullable(context, value, nullable)?
        }
    })
}

fn resolve_from_unixtime(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
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
        )?
    })
}

pub(crate) fn integer_part(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    part: &str,
) -> PlanResult<Expr> {
    let part = context.literal(ScalarValue::Utf8(Some(part.to_uppercase())));
    let result = context.scalar("date_part", vec![part, value])?;
    context.cast(result, DataType::Int32)
}

pub(crate) fn trunc_part(context: &mut FunctionContextInput<'_>, part: Expr) -> PlanResult<Expr> {
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

pub(crate) fn declare_nullable(
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

pub(crate) fn resolve_to_date(
    context: &mut FunctionContextInput<'_>,
    values: Vec<Expr>,
) -> PlanResult<Expr> {
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
        return declare_nullable(context, value, nullable);
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

pub(crate) fn resolve_to_timestamp(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    values: Vec<Expr>,
) -> PlanResult<Expr> {
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

pub(crate) mod datetime_arithmetic;
pub(crate) mod time_window;
pub(crate) mod timezone;

pub(super) fn list_datetime_helpers() -> Vec<(&'static str, crate::function::common::ScalarFunction)>
{
    let mut output = timezone::list_built_in_timezone_functions();
    output.extend(datetime_arithmetic::list_built_in_datetime_arithmetic_functions());
    output
}

pub(super) fn list_built_in_datetime_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        ("current_date", F::kernel("current_date")),
        ("current_time", F::kernel("current_time")),
        (
            "last_day",
            F::udf(sail_function::scalar::datetime::spark_last_day::SparkLastDay::new()),
        ),
        (
            "make_dt_interval",
            F::udf(
                datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval::new(),
            ),
        ),
        (
            "make_interval",
            F::udf(datafusion_spark::function::datetime::make_interval::SparkMakeInterval::new()),
        ),
        (
            "make_time",
            F::udf(sail_function::scalar::datetime::spark_make_time::SparkMakeTime::new()),
        ),
        (
            "make_ym_interval",
            F::udf(
                sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval::new(),
            ),
        ),
        ("session_window", F::kernel("session_window")),
        ("time_bucket", F::kernel("time_bucket")),
        (
            "time_diff",
            F::udf(sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff::new()),
        ),
        ("time_from_micros", F::kernel("time_from_micros")),
        ("time_from_millis", F::kernel("time_from_millis")),
        ("time_from_seconds", F::kernel("time_from_seconds")),
        ("time_to_micros", F::kernel("time_to_micros")),
        ("time_to_millis", F::kernel("time_to_millis")),
        ("time_to_seconds", F::kernel("time_to_seconds")),
        (
            "time_trunc",
            F::udf(sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc::new()),
        ),
        (
            "try_make_interval",
            F::udf(datafusion_spark::function::datetime::make_interval::SparkMakeInterval::new()),
        ),
        ("window", F::kernel("window")),
        (
            "year",
            F::udf(sail_function::scalar::datetime::spark_year::SparkYear::new()),
        ),
        ("curdate", F::custom(resolve_curdate)),
        ("window_time", F::custom(resolve_window_time)),
        ("current_timezone", F::custom(resolve_current_timezone)),
        (
            "current_timestamp",
            F::custom(|input| resolve_current_timestamp(input, "current_timestamp")),
        ),
        (
            "now",
            F::custom(|input| resolve_current_timestamp(input, "now")),
        ),
        (
            "localtimestamp",
            F::custom(|input| resolve_current_timestamp(input, "localtimestamp")),
        ),
        (
            "date_format",
            F::custom(|input| resolve_date_format(input, "date_format")),
        ),
        ("date_part", F::custom(resolve_date_part)),
        ("datepart", F::custom(resolve_date_part)),
        ("extract", F::custom(resolve_date_part)),
        (
            "dayname",
            F::custom(|input| resolve_dayname(input, "dayname")),
        ),
        (
            "monthname",
            F::custom(|input| resolve_dayname(input, "monthname")),
        ),
        (
            "weekofyear",
            F::custom(|input| resolve_dayname(input, "weekofyear")),
        ),
        (
            "date_trunc",
            F::custom(|input| resolve_date_trunc(input, "date_trunc")),
        ),
        (
            "trunc",
            F::custom(|input| resolve_date_trunc(input, "trunc")),
        ),
        (
            "date_from_unix_date",
            F::custom(resolve_date_from_unix_date),
        ),
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
        ("next_day", F::custom(resolve_next_day)),
        (
            "to_time",
            F::custom(|input| resolve_to_time(input, "to_time")),
        ),
        (
            "try_to_time",
            F::custom(|input| resolve_to_time(input, "try_to_time")),
        ),
        ("to_date", F::custom(to_date)),
        (
            "to_timestamp",
            F::custom(|input| to_timestamp(input, "to_timestamp")),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, "to_timestamp_ntz")),
        ),
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, "to_timestamp_ltz")),
        ),
        (
            "try_to_timestamp",
            F::custom(|input| to_timestamp(input, "try_to_timestamp")),
        ),
        (
            "unix_timestamp",
            F::custom(|input| resolve_unix_timestamp(input, "unix_timestamp")),
        ),
        (
            "to_unix_timestamp",
            F::custom(|input| resolve_unix_timestamp(input, "to_unix_timestamp")),
        ),
        ("from_unixtime", F::custom(resolve_from_unixtime)),
        (
            "years",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "YEAR")?;
                Ok(result)
            }),
        ),
        (
            "day",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DAY")?;
                Ok(result)
            }),
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
            "weekday",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DOW")?;
                let one = input.context.literal(ScalarValue::Int32(Some(1)));
                input.context.binary(result, Operator::Minus, one)
            }),
        ),
        (
            "dayofyear",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "DOY")?;
                Ok(result)
            }),
        ),
        (
            "hour",
            F::custom(|mut input| {
                let result = integer_part(&mut input.context, input.arguments.one()?, "HOUR")?;
                Ok(result)
            }),
        ),
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
    ]
}
