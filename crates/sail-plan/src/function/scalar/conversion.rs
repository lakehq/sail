use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use chumsky::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::spark_to_csv::SparkToCsv;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::json::{
    SparkFromJson, json_as_text_udf, json_length_udf, json_object_keys_udf, to_json_udf,
};
use sail_function::scalar::xml::from_xml::SparkFromXml;
use sail_function::scalar::xml::to_xml::SparkToXml;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunctionBuilder as F, ScalarFunctionInput};

fn resolve_date(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        let value = values.one()?;
        if value
            .result_type
            .resolved("conversion input")?
            .data_type
            .is_string()
        {
            let function = match name {
                "date" => ScalarUDF::from(SparkDate::new(false)),
                "time" => ScalarUDF::from(SparkTime::new(false)),
                _ => ScalarUDF::from(SparkTimestamp::try_new(
                    Some(timezone),
                    context.config.ansi_mode,
                    false,
                )?),
            };
            context.scalar_udf(Arc::new(function), vec![value])?
        } else {
            let target = match name {
                "date" => DataType::Date32,
                "time" => DataType::Time64(TimeUnit::Microsecond),
                _ => DataType::Timestamp(TimeUnit::Microsecond, None),
            };
            context.cast(value, target)?
        }
    })
}

fn resolve_from_csv(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    Ok({
        if let Some(schema) = values.get_mut(1)
            && !matches!(schema.kind, Deferred::Resolved(ExprKind::Literal(_)))
            && let Ok(value) = crate::analyzer::lowering::evaluate_constant(schema)
        {
            let value = if name == "from_xml" {
                match value {
                    ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => ScalarValue::Utf8(v),
                    value => value,
                }
            } else {
                value
            };
            *schema = context.literal(value);
        }
        let function = match name {
            "from_csv" => ScalarUDF::from(SparkFromCSV::new(timezone)),
            "from_json" => ScalarUDF::from(SparkFromJson::new(timezone)),
            _ => ScalarUDF::from(SparkFromXml::new(timezone)),
        };
        context.scalar_udf(Arc::new(function), values)?
    })
}

fn resolve_to_csv(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkToCsv::new(timezone))), values)
}

fn resolve_to_json(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        if !matches!(values.len(), 1 | 2) {
            return Err(PlanError::invalid(format!(
                "to_json expects 1 or 2 arguments, got {}",
                values.len()
            )));
        }
        context.scalar_udf(to_json_udf(), values)?
    })
}

fn resolve_to_xml(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkToXml::new(timezone))), values)
}

fn resolve_get_json_object(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (value, path) = values.two()?;
        let paths = if let Deferred::Resolved(ExprKind::Literal(ScalarValue::Utf8(Some(path)))) =
            &path.kind
        {
            let Some(paths) = json_path_parser().parse(path).into_result().ok() else {
                return Ok(context.literal(ScalarValue::Utf8(None)));
            };
            paths
                .into_iter()
                .map(|v| context.literal(v))
                .collect::<Vec<_>>()
        } else {
            let copy = context.copy_expression(&path)?;
            let pattern = context.literal(ScalarValue::Utf8(Some("$.%".into())));
            let matches = crate::function::scalar::predicate::pattern(
                context, copy, pattern, false, false, None,
            )?;
            let three = context.literal(ScalarValue::Int64(Some(3)));
            let key = context.scalar("substr", vec![path, three])?;
            let yes = context.literal(ScalarValue::Boolean(Some(true)));
            let empty = context.literal(ScalarValue::Utf8(Some("".into())));
            vec![context.case(vec![(matches, key), (yes, empty)], None)?]
        };
        let mut arguments = vec![value];
        arguments.extend(paths);
        context.scalar_udf(json_as_text_udf(), arguments)?
    })
}

fn resolve_json_array_length(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = context.scalar_udf(json_length_udf(), vec![values.one()?])?;
        context.cast(value, DataType::Int32)?
    })
}

fn resolve_json_object_keys(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(json_object_keys_udf(), vec![values.one()?])
}

fn json_path_parser<'src>()
-> impl Parser<'src, &'src str, Vec<ScalarValue>, extra::Err<Rich<'src, char>>> {
    // `.key` — a run of characters other than `.` or `[`.
    let dot_key = just('.')
        .ignore_then(
            none_of(['.', '['])
                .repeated()
                .at_least(1)
                .collect::<String>(),
        )
        .map(|value| ScalarValue::Utf8(Some(value)));

    // `['key']` — single-quoted (may contain dots). Double-quoted brackets are
    // not accepted, matching Spark (which returns NULL for them).
    let bracket_key = just('[')
        .ignore_then(just('\''))
        .ignore_then(none_of('\'').repeated().collect::<String>())
        .then_ignore(just('\''))
        .then_ignore(just(']'))
        .map(|value| ScalarValue::Utf8(Some(value)));

    // `[0]` — array index.
    let index = just('[')
        .ignore_then(text::int(10).try_map(|digits: &str, span| {
            digits
                .parse::<i64>()
                .map(|value| ScalarValue::Int64(Some(value)))
                .map_err(|e| Rich::custom(span, e.to_string()))
        }))
        .then_ignore(just(']'));

    just('$')
        .ignore_then(
            choice((dot_key, bracket_key, index))
                .repeated()
                .collect::<Vec<_>>(),
        )
        .then_ignore(end())
}

pub(super) fn list_built_in_conversion_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        ("cast", F::kernel("cast")),
        ("date", F::custom(|input| resolve_date(input, "date"))),
        ("time", F::custom(|input| resolve_date(input, "time"))),
        (
            "timestamp",
            F::custom(|input| resolve_date(input, "timestamp")),
        ),
        (
            "from_csv",
            F::custom(|input| resolve_from_csv(input, "from_csv")),
        ),
        ("to_csv", F::custom(resolve_to_csv)),
        (
            "from_json",
            F::custom(|input| resolve_from_csv(input, "from_json")),
        ),
        ("to_json", F::custom(resolve_to_json)),
        (
            "from_xml",
            F::custom(|input| resolve_from_csv(input, "from_xml")),
        ),
        ("to_xml", F::custom(resolve_to_xml)),
        ("get_json_object", F::custom(resolve_get_json_object)),
        ("json_array_length", F::custom(resolve_json_array_length)),
        ("json_object_keys", F::custom(resolve_json_object_keys)),
        ("bigint", F::cast(DataType::Int64)),
        ("binary", F::cast(DataType::Binary)),
        ("boolean", F::cast(DataType::Boolean)),
        ("decimal", F::cast(DataType::Decimal128(10, 0))),
        ("double", F::cast(DataType::Float64)),
        ("float", F::cast(DataType::Float32)),
        ("int", F::cast(DataType::Int32)),
        ("smallint", F::cast(DataType::Int16)),
        ("string", F::cast(DataType::Utf8)),
        ("tinyint", F::cast(DataType::Int8)),
    ]
}
