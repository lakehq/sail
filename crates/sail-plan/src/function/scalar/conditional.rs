use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_timezone_cast::SparkTimezoneCast;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

pub(super) fn coerce_temporal_values(
    mut arguments: Vec<expr::Expr>,
    value_indices: &[usize],
    function_context: &crate::function::common::FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let mut data_types = Vec::with_capacity(value_indices.len());
    for &index in value_indices {
        let Some(argument) = arguments.get(index) else {
            // Leave malformed arity to the function-specific `ItemTaker` check below.
            return Ok(arguments);
        };
        data_types.push(argument.get_type(function_context.schema)?);
    }
    if !data_types.iter().any(is_temporal_type)
        || !data_types.iter().all(|data_type| {
            is_temporal_type(data_type) || is_string_type(data_type) || data_type.is_null()
        })
    {
        return Ok(arguments);
    }

    let has_string = data_types.iter().any(is_string_type);
    if has_string && !function_context.plan_config.ansi_mode {
        for (&index, data_type) in value_indices.iter().zip(data_types) {
            if is_temporal_type(&data_type) {
                arguments[index] = ScalarUDF::from(SparkToUtf8::new(
                    function_context.plan_config.session_timezone.clone(),
                ))
                .call(vec![arguments[index].clone()]);
            }
        }
        return Ok(arguments);
    }

    let Some(target_type) = common_temporal_type(&data_types) else {
        return Ok(arguments);
    };
    for (&index, data_type) in value_indices.iter().zip(data_types) {
        arguments[index] = coerce_to_temporal(
            arguments[index].clone(),
            &data_type,
            &target_type,
            &function_context.plan_config.session_timezone,
        )?;
    }
    Ok(arguments)
}

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut value_indices = (1..arguments.len()).step_by(2).collect::<Vec<_>>();
    if arguments.len() % 2 == 1 {
        value_indices.push(arguments.len() - 1);
    }
    let arguments = coerce_temporal_values(arguments, &value_indices, &function_context)?;
    let mut when_then_expr = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                when_then_expr.push((Box::new(condition), Box::new(result)));
            }
            _ => {
                when_then_expr.push((Box::new(lit(true)), Box::new(condition)));
                break;
            }
        }
    }
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_temporal_values(arguments, &[1, 2], &function_context)?;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let indices = (0..arguments.len()).collect::<Vec<_>>();
    let arguments = coerce_temporal_values(arguments, &indices, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

fn ifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_temporal_values(arguments, &[0, 1], &function_context)?;
    let (left, right) = arguments.two()?;
    Ok(expr_fn::nvl(left, right))
}

fn nvl2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_temporal_values(arguments, &[1, 2], &function_context)?;
    let (test, value_if_not_null, value_if_null) = arguments.three()?;
    Ok(expr_fn::nvl2(test, value_if_not_null, value_if_null))
}

fn nullif(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (left, right) = input.arguments.two()?;
    let (left, right) = super::predicate::coerce_temporal_comparison(
        left,
        right,
        input.function_context.schema,
        input.function_context.plan_config,
    )?;
    Ok(expr_fn::nullif(left, right))
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
    session_timezone: &Arc<str>,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if data_type.is_null() {
        return Ok(cast(arg, target_type.clone()));
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // Only reached on the ANSI-enabled coalesce path, so strict parsing.
            DataType::Timestamp(_, timezone) => Ok(ScalarUDF::from(SparkTimestamp::try_new(
                timezone.as_ref().map(|_| Arc::clone(session_timezone)),
                true,
                false,
            )?)
            .call(vec![arg])),
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if is_temporal_type(data_type) {
        if matches!(
            (data_type, target_type),
            (
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
                DataType::Timestamp(_, Some(_)),
            ) | (
                DataType::Timestamp(_, Some(_)),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
            ) | (
                DataType::Timestamp(_, Some(_)),
                DataType::Timestamp(_, Some(_)),
            )
        ) {
            Ok(ScalarUDF::from(SparkTimezoneCast::new(
                target_type.clone(),
                Arc::clone(session_timezone),
                false,
            ))
            .call(vec![arg]))
        } else {
            Ok(cast(arg, target_type.clone()))
        }
    } else {
        Ok(arg)
    }
}

pub(super) fn common_temporal_type(data_types: &[DataType]) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn collection_value_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(field.data_type()),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return None;
            };
            fields.first().map(|field| field.data_type())
        }
        _ => None,
    }
}

fn with_collection_value_type(data_type: &DataType, value_type: DataType) -> Option<DataType> {
    let replace = |field: &arrow::datatypes::FieldRef| {
        Arc::new(field.as_ref().clone().with_data_type(value_type.clone()))
    };
    match data_type {
        DataType::List(field) => Some(DataType::List(replace(field))),
        DataType::LargeList(field) => Some(DataType::LargeList(replace(field))),
        DataType::ListView(field) => Some(DataType::ListView(replace(field))),
        DataType::LargeListView(field) => Some(DataType::LargeListView(replace(field))),
        DataType::FixedSizeList(field, size) => {
            Some(DataType::FixedSizeList(replace(field), *size))
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return None;
            };
            let mut fields = fields.iter().cloned().collect::<Vec<_>>();
            let key = fields.first_mut()?;
            *key = replace(key);
            Some(DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(fields.into())),
                ),
                *sorted,
            ))
        }
        _ => None,
    }
}

pub(super) fn coerce_temporal_collection_value(
    collection: expr::Expr,
    value: expr::Expr,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    coerce_temporal_collection_comparison(
        collection,
        value,
        function_context.schema,
        function_context.plan_config,
    )
}

pub(crate) fn coerce_temporal_collection_comparison(
    collection: expr::Expr,
    value: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let collection_type = collection.get_type(schema)?;
    let value_type = value.get_type(schema)?;
    let Some(item_type) = collection_value_type(&collection_type) else {
        return Ok((collection, value));
    };
    if !is_temporal_type(item_type) || !(is_temporal_type(&value_type) || value_type.is_null()) {
        return Ok((collection, value));
    }
    let Some(target_type) = common_temporal_type(&[item_type.clone(), value_type.clone()]) else {
        return Ok((collection, value));
    };
    let collection = if item_type == &target_type {
        collection
    } else {
        let target_collection_type =
            with_collection_value_type(&collection_type, target_type.clone()).ok_or_else(|| {
                crate::error::PlanError::invalid("unsupported temporal collection type")
            })?;
        ScalarUDF::from(SparkTimezoneCast::new(
            target_collection_type,
            config.session_timezone.clone(),
            false,
        ))
        .call(vec![collection])
    };
    let value = super::predicate::coerce_temporal_expr(value, &value_type, &target_type, config)?;
    Ok((collection, value))
}

pub(super) fn coerce_temporal_collections(
    left: expr::Expr,
    right: expr::Expr,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let arguments = coerce_temporal_collection_values(vec![left, right], function_context)?;
    Ok(arguments.two()?)
}

pub(super) fn coerce_temporal_collection_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let collection_types = arguments
        .iter()
        .map(|argument| argument.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let item_types = collection_types
        .iter()
        .map(|data_type| collection_value_type(data_type).cloned())
        .collect::<Option<Vec<_>>>();
    let Some(item_types) = item_types else {
        return Ok(arguments);
    };
    if !item_types.iter().all(|item| is_temporal_type(item)) {
        return Ok(arguments);
    }
    let Some(target_type) = common_temporal_type(&item_types) else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .zip(collection_types)
        .zip(item_types)
        .map(|((argument, collection_type), item_type)| {
            if item_type == target_type {
                Ok(argument)
            } else {
                let target_collection_type =
                    with_collection_value_type(&collection_type, target_type.clone()).ok_or_else(
                        || crate::error::PlanError::invalid("unsupported temporal collection type"),
                    )?;
                Ok(ScalarUDF::from(SparkTimezoneCast::new(
                    target_collection_type,
                    function_context.plan_config.session_timezone.clone(),
                    false,
                ))
                .call(vec![argument]))
            }
        })
        .collect()
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::custom(ifnull)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::custom(nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(ifnull)),
        ("nvl2", F::custom(nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

/// Create a zero literal with the same type as the input expression
fn create_zero_literal(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(Some(0), *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            ScalarValue::Decimal256(Some(0.into()), *precision, *scale)
        }
        // For non-numeric types, default to Int32
        _ => ScalarValue::Int32(Some(0)),
    }
}

/// Implementation of nullifzero function with type-aware casting
fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nullif(arg, zero_literal)
    Ok(expr_fn::nullif(arg, zero_literal))
}

/// Implementation of zeroifnull function with type-aware casting
fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nvl(arg, zero_literal)
    Ok(expr_fn::nvl(arg, zero_literal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_temporal_type_canonicalizes_ltz_metadata() {
        assert_eq!(
            common_temporal_type(&[DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("+01:02:03")),
            )]),
            Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ))
        );
        assert_eq!(
            common_temporal_type(&[DataType::Timestamp(TimeUnit::Second, None)]),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
    }
}
