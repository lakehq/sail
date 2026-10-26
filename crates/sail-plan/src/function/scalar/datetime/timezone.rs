use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

use crate::analyzer::model::*;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_convert_timezone(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    Ok({
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
        let (timestamp, _) = ntz_timestamp(context, timestamp, false)?;
        let args = vec![from, to, timestamp];
        let mut eager = false;
        for arg in &args {
            eager |= arg.any(&|node| match node {
                    ExprKind::ScalarFunction { function, .. } => function.inner().is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>() || function.inner().is::<sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant>(),
                    _ => false,
                })?;
        }
        let function = ConvertTz::new(true).with_null_short_circuit();
        if eager {
            context.scalar_udf(Arc::new(ScalarUDF::from(function)), args)?
        } else {
            crate::function::scalar::sequence::lazy_arguments(
                context,
                Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(function))),
                args,
            )?
        }
    })
}

fn resolve_from_utc_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    Ok({
        let (timestamp, zone) = values.two()?;
        let (timestamp, unit) = ntz_timestamp(context, timestamp, true)?;
        let utc = context.literal(ScalarValue::Utf8(Some("UTC".into())));
        let args = if name == "from_utc_timestamp" {
            vec![utc, zone, timestamp]
        } else {
            vec![zone, utc, timestamp]
        };
        let result = context.scalar_udf(Arc::new(ScalarUDF::from(ConvertTz::new(false))), args)?;
        let result = context.cast(result, DataType::Timestamp(unit, Some("UTC".into())))?;
        context.cast(result, DataType::Timestamp(unit, Some(session_tz)))?
    })
}

fn resolve_make_timestamp(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let session_tz = context.config.session_timezone.clone();
    Ok({
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
        result
    })
}

pub(crate) fn ntz_timestamp(
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

pub(in crate::function::scalar) fn list_built_in_timezone_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        ("convert_timezone", F::custom(resolve_convert_timezone)),
        (
            "from_utc_timestamp",
            F::custom(|input| resolve_from_utc_timestamp(input, "from_utc_timestamp")),
        ),
        (
            "to_utc_timestamp",
            F::custom(|input| resolve_from_utc_timestamp(input, "to_utc_timestamp")),
        ),
        (
            "make_timestamp",
            F::custom(|input| resolve_make_timestamp(input, "make_timestamp")),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| resolve_make_timestamp(input, "make_timestamp_ltz")),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| resolve_make_timestamp(input, "make_timestamp_ntz")),
        ),
        (
            "try_make_timestamp",
            F::custom(|input| resolve_make_timestamp(input, "try_make_timestamp")),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| resolve_make_timestamp(input, "try_make_timestamp_ltz")),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| resolve_make_timestamp(input, "try_make_timestamp_ntz")),
        ),
    ]
}
