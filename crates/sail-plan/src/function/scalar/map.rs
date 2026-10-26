use std::sync::Arc;

use arrow_schema::{DataType, FieldRef};
use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::map::map_entries::SparkMapEntries;
use sail_function::scalar::map::map_from::{SparkMapFromArrays, SparkMapFromEntries};
use sail_function::scalar::map::str_to_map::StrToMap;

use crate::analyzer::model::Expr;
use crate::config::MapKeyDedupPolicy;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::function::scalar::array::cast_list_value_nullability;

fn map(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
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
    let keys = coerce_ansi_map_strings(context, keys)?;
    let entries = coerce_ansi_map_strings(context, entries)?;
    let nullable = entries.iter().try_fold(false, |nullable, v| {
        Ok::<_, PlanError>(nullable || v.result_type.resolved("map value")?.nullable)
    })?;
    let function = Arc::new(ScalarUDF::from(SparkArray::new()));
    let keys = context.scalar_udf(function.clone(), keys)?;
    let entries = context.scalar_udf(function, entries)?;
    let entries = cast_list_value_nullability(context, entries, true)?;
    let result = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
        vec![keys, entries],
    )?;
    cast_map_value_nullability(context, result, nullable)
}

pub(crate) fn coerce_ansi_map_strings(
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
            ansi_map_common_type(&previous, data_type)
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

fn ansi_map_common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right || right.is_null() {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if left.is_string() != right.is_string() {
        let atomic = if left.is_string() { right } else { left };
        return if atomic.is_integer() {
            Some(DataType::Int64)
        } else if atomic.is_numeric() {
            Some(DataType::Float64)
        } else if atomic.is_nested()
            || matches!(atomic, DataType::Interval(_) | DataType::Duration(_))
        {
            None
        } else {
            Some(atomic.clone())
        };
    }
    if left.is_numeric() && right.is_numeric() && (left.is_floating() || right.is_floating()) {
        return Some(DataType::Float64);
    }
    comparison_coercion(left, right)
}

fn map_from_arrays(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    let (keys, values) = values.two()?;
    let nullable = match &values.result_type.resolved("map values")?.data_type {
        DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
        _ => true,
    };
    let values = cast_list_value_nullability(context, values, true)?;
    let result = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
        vec![keys, values],
    )?;
    cast_map_value_nullability(context, result, nullable)
}

fn map_from_entries(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    let entries = values.one()?;
    let data_type = &entries.result_type.resolved("map entries")?.data_type;
    let nullable = map_entries_value_contains_null(data_type);
    let target = map_entries_nullability(data_type, true);
    let entries = context.cast(entries, target)?;
    let result = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapFromEntries::new(last_wins))),
        vec![entries],
    )?;
    cast_map_value_nullability(context, result, nullable)
}

fn map_entries(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapEntries::new())),
        vec![values.one()?],
    )
}

fn map_values(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let nullable = map_value_contains_null(&value.result_type.resolved("map value")?.data_type);
    let value = cast_map_value_nullability(context, value, true)?;
    let value = context.scalar_udf(functions_nested::map_values::map_values_udf(), vec![value])?;
    cast_list_value_nullability(context, value, nullable)
}

fn map_value_contains_null(data_type: &DataType) -> bool {
    let DataType::Map(entries, _) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

fn map_entries_value_contains_null(data_type: &DataType) -> bool {
    let (DataType::List(field) | DataType::LargeList(field)) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = field.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

fn cast_map_entry_field_value_nullability(field: &FieldRef, nullable: bool) -> FieldRef {
    let DataType::Struct(fields) = field.data_type() else {
        return field.clone();
    };
    if fields.len() != 2 || fields[1].is_nullable() == nullable {
        return field.clone();
    }
    Arc::new(field.as_ref().clone().with_data_type(DataType::Struct(
        vec![fields[0].clone(), with_nullable(&fields[1], nullable)].into(),
    )))
}

pub(crate) fn cast_map_value_nullability(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    nullable: bool,
) -> PlanResult<Expr> {
    let target = map_nullability(
        &value.result_type.resolved("map input")?.data_type,
        nullable,
    );
    context.cast(value, target)
}

fn with_nullable(field: &FieldRef, nullable: bool) -> FieldRef {
    if field.is_nullable() == nullable {
        field.clone()
    } else {
        Arc::new(field.as_ref().clone().with_nullable(nullable))
    }
}

fn map_concat(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    if values.is_empty() {
        return Ok(context.literal(ScalarValue::Null));
    }
    let mut keys = vec![];
    let mut entries = vec![];
    let mut null = None;
    for map in values {
        let copy = context.copy_expression(&map)?;
        keys.push(context.scalar_udf(functions_nested::map_keys::map_keys_udf(), vec![copy])?);
        let copy = context.copy_expression(&map)?;
        entries
            .push(context.scalar_udf(functions_nested::map_values::map_values_udf(), vec![copy])?);
        let check = context.is_null(map);
        null = Some(match null {
            Some(previous) => context.binary(previous, Operator::Or, check)?,
            None => check,
        });
    }
    let keys = context.scalar_udf(functions_nested::concat::array_concat_udf(), keys)?;
    let entries = context.scalar_udf(functions_nested::concat::array_concat_udf(), entries)?;
    let value = context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkMapFromArrays::new(last_wins))),
        vec![keys, entries],
    )?;
    let condition = null.ok_or_else(|| PlanError::internal("map_concat arguments missing"))?;
    let null = context.literal(ScalarValue::Null);
    context.case(vec![(condition, null)], Some(value))
}

fn map_contains_key(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (map, key) = values.two()?;
    let keys = context.scalar_udf(functions_nested::map_keys::map_keys_udf(), vec![map])?;
    context.scalar_udf(
        functions_nested::array_has::array_has_udf(),
        vec![keys, key],
    )
}

fn str_to_map(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let last_wins = context.config.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
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
    )
}

fn map_entries_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::List(field) => {
            DataType::List(cast_map_entry_field_value_nullability(field, nullable))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(cast_map_entry_field_value_nullability(field, nullable))
        }
        _ => data_type.clone(),
    }
}

fn map_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::Map(field, sorted) => DataType::Map(
            cast_map_entry_field_value_nullability(field, nullable),
            *sorted,
        ),
        _ => data_type.clone(),
    }
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("map", F::custom(map)),
        ("map_concat", F::custom(map_concat)),
        ("map_contains_key", F::custom(map_contains_key)),
        ("map_entries", F::custom(map_entries)),
        ("map_from_arrays", F::custom(map_from_arrays)),
        ("map_from_entries", F::custom(map_from_entries)),
        (
            "map_keys",
            F::scalar_udf(functions_nested::map_keys::map_keys_udf),
        ),
        ("map_values", F::custom(map_values)),
        ("str_to_map", F::custom(str_to_map)),
    ]
}
