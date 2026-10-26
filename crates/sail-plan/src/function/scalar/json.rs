use std::sync::Arc;

use arrow_schema::DataType;
use chumsky::prelude::*;
use datafusion::functions::unicode;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::json::{
    SparkFromJson, SparkSchemaOfJson, json_as_text_udf, json_length_udf, json_object_keys_udf,
    to_json_udf,
};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

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

fn get_json_object(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, path) = values.two()?;
    let paths =
        if let Deferred::Resolved(ExprKind::Literal(ScalarValue::Utf8(Some(path)))) = &path.kind {
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
            let key = context.scalar_udf(unicode::substr(), vec![path, three])?;
            let yes = context.literal(ScalarValue::Boolean(Some(true)));
            let empty = context.literal(ScalarValue::Utf8(Some("".into())));
            vec![context.case(vec![(matches, key), (yes, empty)], None)?]
        };
    let mut arguments = vec![value];
    arguments.extend(paths);
    context.scalar_udf(json_as_text_udf(), arguments)
}

fn json_array_length(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = context.scalar_udf(json_length_udf(), vec![values.one()?])?;
    context.cast(value, DataType::Int32)
}

fn json_object_keys(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(json_object_keys_udf(), vec![values.one()?])
}

fn to_json(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    if !matches!(values.len(), 1 | 2) {
        return Err(PlanError::invalid(format!(
            "to_json expects 1 or 2 arguments, got {}",
            values.len()
        )));
    }
    context.scalar_udf(to_json_udf(), values)
}

fn from_json(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    if let Some(schema) = values.get_mut(1)
        && !matches!(schema.kind, Deferred::Resolved(ExprKind::Literal(_)))
        && let Ok(value) = crate::analyzer::lowering::evaluate_constant(schema)
    {
        *schema = context.literal(value);
    }
    let function = ScalarUDF::from(SparkFromJson::new(timezone));
    context.scalar_udf(Arc::new(function), values)
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_json", F::custom(from_json)),
        ("get_json_object", F::custom(get_json_object)),
        ("json_array_length", F::custom(json_array_length)),
        ("json_object_keys", F::custom(json_object_keys)),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::udf(SparkSchemaOfJson::new())),
        ("to_json", F::custom(to_json)),
    ]
}
