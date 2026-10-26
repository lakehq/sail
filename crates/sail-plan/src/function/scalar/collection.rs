use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::map::map_entries::SparkMapEntries;
use sail_function::scalar::map::map_from::{SparkMapFromArrays, SparkMapFromEntries};
use sail_function::scalar::map::str_to_map::StrToMap;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::analyzer::model::*;
use crate::config::MapKeyDedupPolicy;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = values.one()?;
        let data_type = &value.result_type.resolved("size input")?.data_type;
        let function = if data_type.is_list() {
            "array_length"
        } else if matches!(data_type, DataType::Map(..)) {
            "cardinality"
        } else {
            return Err(PlanError::invalid(format!(
                "size expects List or Map as argument, got {data_type:?}"
            )));
        };
        let result = context.scalar(function, vec![value])?;
        let result = context.cast(result, DataType::Int32)?;
        if context.config.legacy_size_of_null && !context.config.ansi_mode {
            let fallback = context.literal(ScalarValue::Int32(Some(-1)));
            context.scalar("coalesce", vec![result, fallback])?
        } else {
            result
        }
    })
}

fn resolve_deep_size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar("cardinality", vec![values.one()?])
}

fn resolve_element_at(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (collection, element) = values.two()?;
        let data_type = &collection
            .result_type
            .resolved("element_at input")?
            .data_type;
        if data_type.is_list() {
            let index = context.copy_expression(&element)?;
            let zero = context.literal(ScalarValue::Int32(Some(0)));
            let invalid = context.binary(index, Operator::Eq, zero)?;
            let error = raise_error(
                context,
                format!(
                    "{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"
                ),
            )?;
            let index = context.copy_expression(&element)?;
            let absolute = context.scalar("abs", vec![index])?;
            let absolute_copy = context.copy_expression(&absolute)?;
            let one = context.literal(ScalarValue::Int64(Some(1)));
            let below = context.binary(absolute_copy, Operator::Lt, one)?;
            let array = context.copy_expression(&collection)?;
            let length = context.scalar("array_length", vec![array])?;
            let above = context.binary(absolute, Operator::Gt, length)?;
            let outside = context.binary(below, Operator::Or, above)?;
            let fallback = if name == "try_element_at" {
                context.literal(ScalarValue::Null)
            } else {
                raise_error(context, "element_at: the index is out of bounds".into())?
            };
            let value = context.scalar("array_element", vec![collection, element])?;
            let always = context.literal(ScalarValue::Boolean(Some(true)));
            context.case(
                vec![(invalid, error), (outside, fallback), (always, value)],
                None,
            )?
        } else if matches!(data_type, DataType::Map(..)) {
            let values = context.scalar("map_extract", vec![collection, element])?;
            let first = context.literal(ScalarValue::Int64(Some(1)));
            context.scalar("array_element", vec![values, first])?
        } else {
            raise_error(
                context,
                format!("{name} expects List or Map type as first argument, got {data_type:?}"),
            )?
        }
    })
}

fn resolve_map(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    Ok({
        if !values.len().is_multiple_of(2) {
            return Err(PlanError::invalid(format!(
                "map(k1, v1, k2, v2, ...): expect number of args to be multiple of 2, got {}",
                values.len()
            )));
        }
        let mut keys = vec![];
        let mut entries = vec![];
        for (i, value) in values.into_iter().enumerate() {
            if i % 2 == 0 {
                keys.push(value);
            } else {
                entries.push(value);
            }
        }
        let keys = ansi_map_arguments(context, keys)?;
        let entries = ansi_map_arguments(context, entries)?;
        let nullable = entries.iter().try_fold(false, |nullable, v| {
            Ok::<_, PlanError>(nullable || v.result_type.resolved("map value")?.nullable)
        })?;
        let function = Arc::new(ScalarUDF::from(SparkArray::new()));
        let keys = context.scalar_udf(function.clone(), keys)?;
        let entries = context.scalar_udf(function, entries)?;
        let entries = list_nullability(context, entries, true)?;
        let result = context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
            vec![keys, entries],
        )?;
        map_nullability(context, result, nullable)?
    })
}

fn resolve_map_concat(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    Ok({
        if values.is_empty() {
            return Ok(context.literal(ScalarValue::Null));
        }
        let mut keys = vec![];
        let mut entries = vec![];
        let mut null = None;
        for map in values {
            let copy = context.copy_expression(&map)?;
            keys.push(context.scalar("map_keys", vec![copy])?);
            let copy = context.copy_expression(&map)?;
            entries.push(context.scalar("map_values", vec![copy])?);
            let check = context.is_null(map);
            null = Some(match null {
                Some(previous) => context.binary(previous, Operator::Or, check)?,
                None => check,
            });
        }
        let keys = context.scalar("array_concat", keys)?;
        let entries = context.scalar("array_concat", entries)?;
        let value = context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
            vec![keys, entries],
        )?;
        let condition = null.ok_or_else(|| PlanError::internal("map_concat arguments missing"))?;
        let null = context.literal(ScalarValue::Null);
        context.case(vec![(condition, null)], Some(value))?
    })
}

fn resolve_map_contains_key(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (map, key) = values.two()?;
        let keys = context.scalar("map_keys", vec![map])?;
        context.scalar("array_has", vec![keys, key])?
    })
}

fn resolve_map_entries(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapEntries::new())),
        vec![values.one()?],
    )
}

fn resolve_map_from_arrays(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    Ok({
        let (keys, values) = values.two()?;
        let nullable = match &values.result_type.resolved("map values")?.data_type {
            DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
            _ => true,
        };
        let values = list_nullability(context, values, true)?;
        let result = context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
            vec![keys, values],
        )?;
        map_nullability(context, result, nullable)?
    })
}

fn resolve_map_from_entries(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    Ok({
        let entries = values.one()?;
        let data_type = &entries.result_type.resolved("map entries")?.data_type;
        let nullable =
            crate::analyzer::typing::collection::map_entries_value_contains_null(data_type);
        let target = crate::analyzer::typing::collection::map_entries_nullability(data_type, true);
        let entries = context.cast(entries, target)?;
        let result = context.scalar_udf(
            Arc::new(ScalarUDF::from(SparkMapFromEntries::new(last_wins))),
            vec![entries],
        )?;
        map_nullability(context, result, nullable)?
    })
}

fn resolve_map_values(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let value = values.one()?;
        let nullable = crate::analyzer::typing::collection::map_value_contains_null(
            &value.result_type.resolved("map value")?.data_type,
        );
        let value = map_nullability(context, value, true)?;
        let value = context.scalar("map_values", vec![value])?;
        list_nullability(context, value, nullable)?
    })
}

fn resolve_str_to_map(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    Ok({
        let (value, separators) = values.at_least_one()?;
        let mut separators = separators.into_iter();
        let pair = separators
            .next()
            .unwrap_or_else(|| context.literal(ScalarValue::Utf8(Some(",".into()))));
        let entry = separators
            .next()
            .unwrap_or_else(|| context.literal(ScalarValue::Utf8(Some(":".into()))));
        context.scalar_udf(
            Arc::new(ScalarUDF::from(StrToMap::new(last_wins))),
            vec![value, pair, entry],
        )?
    })
}

pub(crate) fn raise_error(
    context: &mut FunctionContextInput<'_>,
    message: String,
) -> PlanResult<Expr> {
    let message = context.literal(ScalarValue::Utf8(Some(message)));
    context.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])
}

pub(crate) fn list_nullability(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    nullable: bool,
) -> PlanResult<Expr> {
    let target = crate::analyzer::typing::collection::list_nullability(
        &value.result_type.resolved("list input")?.data_type,
        nullable,
    );
    context.cast(value, target)
}

pub(crate) fn map_nullability(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    nullable: bool,
) -> PlanResult<Expr> {
    let target = crate::analyzer::typing::collection::map_nullability(
        &value.result_type.resolved("map input")?.data_type,
        nullable,
    );
    context.cast(value, target)
}

pub(crate) fn ansi_map_arguments(
    context: &mut FunctionContextInput<'_>,
    arguments: Vec<Expr>,
) -> PlanResult<Vec<Expr>> {
    let types = arguments
        .iter()
        .map(|a| Ok(a.result_type.resolved("map argument")?.data_type.clone()))
        .collect::<PlanResult<Vec<_>>>()?;
    if !context.config.ansi_mode || !types.iter().any(DataType::is_string) {
        return Ok(arguments);
    }
    let target = types
        .iter()
        .try_fold(DataType::Null, |previous, data_type| {
            crate::analyzer::typing::collection::ansi_map_common_type(&previous, data_type)
        })
        .ok_or_else(|| PlanError::analysis("map arguments have incompatible types"))?;
    arguments
        .into_iter()
        .zip(types)
        .map(|(argument, source)| {
            if source == target {
                return Ok(argument);
            }
            let value = context.cast(argument, target.clone())?;
            if source.is_string() && !target.is_binary() {
                let condition = context.literal(ScalarValue::Boolean(Some(true)));
                context.case(vec![(condition, value)], None)
            } else {
                Ok(value)
            }
        })
        .collect()
}

pub(super) fn list_built_in_collection_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "concat",
            F::udf(sail_function::scalar::collection::spark_concat::SparkConcat::new()),
        ),
        (
            "reverse",
            F::udf(sail_function::scalar::collection::spark_reverse::SparkReverse::new()),
        ),
        ("||", F::kernel("||")),
        ("size", F::custom(resolve_size)),
        ("cardinality", F::custom(resolve_size)),
        ("deep_size", F::custom(resolve_deep_size)),
        (
            "element_at",
            F::custom(|input| resolve_element_at(input, "element_at")),
        ),
        (
            "try_element_at",
            F::custom(|input| resolve_element_at(input, "try_element_at")),
        ),
        ("map", F::custom(resolve_map)),
        ("map_concat", F::custom(resolve_map_concat)),
        ("map_contains_key", F::custom(resolve_map_contains_key)),
        ("map_entries", F::custom(resolve_map_entries)),
        ("map_from_arrays", F::custom(resolve_map_from_arrays)),
        ("map_from_entries", F::custom(resolve_map_from_entries)),
        ("map_values", F::custom(resolve_map_values)),
        ("str_to_map", F::custom(resolve_str_to_map)),
    ]
}
