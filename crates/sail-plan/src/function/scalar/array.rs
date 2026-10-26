use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn resolve_sequence(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    crate::function::scalar::sequence::sequence(context, values)
}

fn resolve_array_insert(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, position, value) = values.three()?;
        insert_array(context, array, position, value)?
    })
}

fn resolve_array_repeat(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (element, count) = values.two()?;
        let count = context.cast(count, DataType::Int64)?;
        let element_copy = context.copy_expression(&element)?;
        let count_copy = context.copy_expression(&count)?;
        let prototype = context.scalar("array_repeat", vec![element_copy, count_copy])?;
        let target = crate::analyzer::typing::collection::nullable_array_type(
            &prototype
                .result_type
                .resolved("array_repeat result")?
                .data_type,
        );
        repeat_array(context, element, count, target)?
    })
}

fn resolve_array_compact(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    compact_array(context, values.one()?)
}

fn resolve_arrays_zip(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let names = input.argument_display_names;

    Ok({
        let fields = values
            .iter()
            .zip(names)
            .enumerate()
            .map(|(i, (v, n))| match v.kind {
                Deferred::Resolved(
                    ExprKind::Attribute(_)
                    | ExprKind::OuterAttribute { .. }
                    | ExprKind::WithMetadata(_),
                ) => n.clone(),
                _ => i.to_string(),
            })
            .collect();
        context.scalar_udf(Arc::new(ScalarUDF::from(ArraysZip::new(fields))), values)?
    })
}

fn resolve_slice(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, start, length) = values.three()?;
        let output_type = array.result_type.resolved("slice input")?.data_type.clone();
        let array = crate::function::scalar::collection::list_nullability(context, array, true)?;
        let start = context.cast(start, DataType::Int64)?;
        let length = context.cast(length, DataType::Int64)?;
        let start_copy = context.copy_expression(&start)?;
        let end = context.binary(start_copy, Operator::Plus, length)?;
        let one = context.literal(ScalarValue::Int64(Some(1)));
        let end = context.binary(end, Operator::Minus, one)?;
        let result = context.scalar("array_slice", vec![array, start, end])?;
        context.cast(result, output_type)?
    })
}

fn resolve_sort_array(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, ascending) = match values.len() {
            1 => (values.one()?, true),
            2 => {
                let (array, order) = values.two()?;
                let ScalarValue::Boolean(Some(order)) =
                    crate::analyzer::lowering::evaluate_constant(&order)?
                else {
                    return Err(PlanError::invalid("Invalid asc value for sort_array"));
                };
                (array, order)
            }
            n => {
                return Err(PlanError::invalid(format!(
                    "sort_array requires 1 or 2 arguments, got {n}"
                )));
            }
        };
        let direction = context.literal(ScalarValue::Utf8(Some(
            if ascending { "ASC" } else { "DESC" }.into(),
        )));
        let nulls = context.literal(ScalarValue::Utf8(Some(
            if ascending {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
            .into(),
        )));
        context.scalar("array_sort", vec![array, direction, nulls])?
    })
}

fn resolve_array_append(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, element) = values.two()?;
        let copy = context.copy_expression(&array)?;
        let condition = context.is_not_null(copy);
        let array = crate::function::scalar::collection::list_nullability(context, array, true)?;
        let value = if name == "array_append" {
            context.scalar(name, vec![array, element])?
        } else {
            context.scalar(name, vec![element, array])?
        };
        let value = crate::function::scalar::collection::list_nullability(context, value, true)?;
        context.case(vec![(condition, value)], None)?
    })
}

fn resolve_get(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, element) = values.two()?;
        let one = context.literal(ScalarValue::Int64(Some(1)));
        let element = context.binary(element, Operator::Plus, one)?;
        context.scalar("array_element", vec![array, element])?
    })
}

fn resolve_array_contains(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, value) = values.two()?;
        let copy = context.copy_expression(&array)?;
        let result = context.scalar("array_has", vec![copy, value])?;
        let present = context.is_not_null(array);
        let no = context.literal(ScalarValue::Boolean(Some(false)));
        let fallback = context.case(vec![(present, no)], None)?;
        context.scalar("coalesce", vec![result, fallback])?
    })
}

fn resolve_array_contains_all(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (array, value) = values.two()?;
        let result = context.scalar("array_has_all", vec![array, value])?;
        let no = context.literal(ScalarValue::Boolean(Some(false)));
        context.scalar("coalesce", vec![result, no])?
    })
}

fn resolve_array_size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let result = context.scalar("array_length", vec![values.one()?])?;
        context.cast(result, DataType::Int32)?
    })
}

fn resolve_array_remove(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (a, b) = values.two()?;
        context.scalar("array_remove_all", vec![a, b])?
    })
}

fn resolve_arrays_overlap(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let (left, right) = values.two()?;
        let target = crate::analyzer::typing::collection::list_nullability(
            &left.result_type.resolved("array overlap left")?.data_type,
            true,
        );
        let left = context.cast(left, target.clone())?;
        let right = context.cast(right, target.clone())?;
        let null = context.literal(ScalarValue::Null);
        let singleton = context.scalar("make_array", vec![null])?;
        let singleton = context.cast(singleton, target)?;
        let singleton_copy = context.copy_expression(&singleton)?;
        let copy = context.copy_expression(&left)?;
        let left_nulls = context.scalar("array_has_any", vec![copy, singleton_copy])?;
        let copy = context.copy_expression(&right)?;
        let right_nulls = context.scalar("array_has_any", vec![copy, singleton])?;
        let has_nulls = context.binary(left_nulls, Operator::Or, right_nulls)?;
        let copy = context.copy_expression(&left)?;
        let left_null = context.is_null(copy);
        let copy = context.copy_expression(&right)?;
        let right_null = context.is_null(copy);
        let either_null = context.binary(left_null, Operator::Or, right_null)?;
        let copy = context.copy_expression(&left)?;
        let compact_left = compact_array(context, copy)?;
        let copy = context.copy_expression(&right)?;
        let compact_right = compact_array(context, copy)?;
        let overlap = context.scalar("array_has_any", vec![compact_left, compact_right])?;
        let null = context.literal(ScalarValue::Null);
        let null_overlap = context.binary(overlap, Operator::Or, null)?;
        let overlap = context.scalar("array_has_any", vec![left, right])?;
        let null = context.literal(ScalarValue::Null);
        let yes = context.literal(ScalarValue::Boolean(Some(true)));
        context.case(
            vec![
                (either_null, null),
                (has_nulls, null_overlap),
                (yes, overlap),
            ],
            None,
        )?
    })
}

fn resolve_flatten(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    Ok({
        let array = values.one()?;
        let target = crate::analyzer::typing::collection::nullable_array_type(
            &array.result_type.resolved("flatten input")?.data_type,
        );
        let array = context.cast(array, target.clone())?;
        let null = context.literal(ScalarValue::Null);
        let singleton = context.scalar("make_array", vec![null])?;
        let singleton = context.cast(singleton, target)?;
        let copy = context.copy_expression(&array)?;
        let has_null = context.scalar("array_has_any", vec![copy, singleton])?;
        let no_null = context.not(has_null)?;
        let flattened = context.scalar("flatten", vec![array])?;
        context.case(vec![(no_null, flattened)], None)?
    })
}

fn resolve_array_sort(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    match input.arguments.len() {
        1 => {
            let array = input.arguments.one()?;
            let direction = input.context.literal(ScalarValue::Utf8(Some("ASC".into())));
            let nulls = input
                .context
                .literal(ScalarValue::Utf8(Some("NULLS LAST".into())));
            input
                .context
                .scalar("array_sort", vec![array, direction, nulls])
        }
        _ => input.context.scalar("array_sort", input.arguments),
    }
}

pub(crate) fn compact_array(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkArrayCompact::new())),
        vec![value],
    )
}

pub(crate) fn repeat_array(
    context: &mut FunctionContextInput<'_>,
    element: Expr,
    count: Expr,
    target: DataType,
) -> PlanResult<Expr> {
    let element_type = crate::analyzer::typing::collection::nullable_array_type(
        &element
            .result_type
            .resolved("array repeat element")?
            .data_type,
    );
    let element = context.cast(element, element_type)?;
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    let element = context.case(vec![(yes, element)], None)?;
    let result = context.scalar("array_repeat", vec![element, count])?;
    context.cast(result, target)
}

pub(crate) fn insert_array(
    context: &mut FunctionContextInput<'_>,
    array: Expr,
    position: Expr,
    value: Expr,
) -> PlanResult<Expr> {
    let copy = context.copy_expression(&array)?;
    let array_input = crate::function::scalar::collection::list_nullability(context, copy, true)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let prototype = context.scalar("array_append", vec![copy, value_copy])?;
    let target = crate::analyzer::typing::collection::list_nullability(
        &prototype
            .result_type
            .resolved("array insert result")?
            .data_type,
        true,
    );
    let array_input = context.cast(array_input, target.clone())?;
    let copy = context.copy_expression(&array)?;
    let length = context.scalar("array_length", vec![copy])?;
    let length = context.cast(length, DataType::Int64)?;
    let copy = context.copy_expression(&position)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let positive = context.binary(copy, Operator::Gt, zero)?;
    let copy = context.copy_expression(&position)?;
    let one = context.literal(ScalarValue::Int32(Some(1)));
    let positive_index = context.binary(copy, Operator::Minus, one)?;
    let copy = context.copy_expression(&position)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let negative = context.binary(copy, Operator::Lt, zero)?;
    let copy = context.copy_expression(&length)?;
    let negative_index = context.binary(copy, Operator::Plus, position)?;
    let one = context.literal(ScalarValue::Int32(Some(1)));
    let negative_index = context.binary(negative_index, Operator::Plus, one)?;
    let index = context.case(
        vec![(positive, positive_index), (negative, negative_index)],
        None,
    )?;
    let copy = context.copy_expression(&array)?;
    let is_null = context.is_null(copy);
    let null_array = context.cast(array, target.clone())?;
    let copy = context.copy_expression(&index)?;
    let invalid = context.is_null(copy);
    let error = crate::function::scalar::collection::raise_error(context, "array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)".into())?;
    let error = context.cast(error, target.clone())?;
    let mut branches = vec![(is_null, null_array), (invalid, error)];

    let copy = context.copy_expression(&index)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let before = context.binary(copy, Operator::Lt, zero)?;
    let copy = context.copy_expression(&value)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let first = repeat_array(context, copy, one, target.clone())?;
    let copy = context.copy_expression(&index)?;
    let zero = context.literal(ScalarValue::Int64(Some(0)));
    let padding_length = context.binary(zero, Operator::Minus, copy)?;
    let null = context.literal(ScalarValue::Null);
    let padding = repeat_array(context, null, padding_length, target.clone())?;
    let copy = context.copy_expression(&array_input)?;
    let result = context.scalar("array_concat", vec![first, padding, copy])?;
    branches.push((before, result));

    let copy = context.copy_expression(&index)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let at_start = context.binary(copy, Operator::Eq, zero)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let result = context.scalar("array_prepend", vec![value_copy, copy])?;
    branches.push((at_start, result));

    let copy = context.copy_expression(&index)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let lower = context.binary(copy, Operator::GtEq, one)?;
    let copy = context.copy_expression(&length)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let upper_limit = context.binary(copy, Operator::Minus, one)?;
    let copy = context.copy_expression(&index)?;
    let upper = context.binary(copy, Operator::LtEq, upper_limit)?;
    let middle = context.binary(lower, Operator::And, upper)?;
    let copy = context.copy_expression(&array_input)?;
    let end = context.copy_expression(&index)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let prefix = context.scalar("array_slice", vec![copy, one, end])?;
    let copy = context.copy_expression(&value)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let inserted = repeat_array(context, copy, one, target.clone())?;
    let copy = context.copy_expression(&index)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let start = context.binary(copy, Operator::Plus, one)?;
    let copy = context.copy_expression(&array_input)?;
    let end = context.copy_expression(&length)?;
    let suffix = context.scalar("array_slice", vec![copy, start, end])?;
    let result = context.scalar("array_concat", vec![prefix, inserted, suffix])?;
    branches.push((middle, result));

    let copy = context.copy_expression(&index)?;
    let end = context.copy_expression(&length)?;
    let at_end = context.binary(copy, Operator::Eq, end)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let result = context.scalar("array_append", vec![copy, value_copy])?;
    branches.push((at_end, result));

    let copy = context.copy_expression(&index)?;
    let end = context.copy_expression(&length)?;
    let beyond = context.binary(copy, Operator::Gt, end)?;
    let padding_length = context.binary(index, Operator::Minus, length)?;
    let null = context.literal(ScalarValue::Null);
    let padding = repeat_array(context, null, padding_length, target.clone())?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let last = repeat_array(context, value, one, target)?;
    let result = context.scalar("array_concat", vec![array_input, padding, last])?;
    branches.push((beyond, result));
    context.case(branches, None)
}

pub(super) fn list_built_in_array_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "array",
            F::udf(sail_function::scalar::array::spark_array::SparkArray::new()),
        ),
        (
            "array_concat",
            F::udf(sail_function::scalar::collection::spark_concat::SparkConcat::new()),
        ),
        ("array_distinct", F::kernel("array_distinct")),
        ("array_except", F::kernel("array_except")),
        (
            "array_intersect",
            F::udf(sail_function::scalar::array::array_intersect::ArrayIntersect::new()),
        ),
        (
            "array_join",
            F::udf(datafusion_functions_nested::string::ArrayToString::new()),
        ),
        (
            "array_max",
            F::udf(sail_function::scalar::array::spark_array_min_max::ArrayMax::new()),
        ),
        (
            "array_min",
            F::udf(sail_function::scalar::array::spark_array_min_max::ArrayMin::new()),
        ),
        (
            "array_position",
            F::udf(sail_function::scalar::array::array_position::SparkArrayPosition::new()),
        ),
        ("array_union", F::kernel("array_union")),
        (
            "shuffle",
            F::scalar_udf(datafusion_spark::function::array::shuffle),
        ),
        ("sequence", F::custom(resolve_sequence)),
        ("array_insert", F::custom(resolve_array_insert)),
        ("array_repeat", F::custom(resolve_array_repeat)),
        ("array_compact", F::custom(resolve_array_compact)),
        ("arrays_zip", F::custom(resolve_arrays_zip)),
        ("slice", F::custom(resolve_slice)),
        ("sort_array", F::custom(resolve_sort_array)),
        (
            "array_append",
            F::custom(|input| resolve_array_append(input, "array_append")),
        ),
        (
            "array_prepend",
            F::custom(|input| resolve_array_append(input, "array_prepend")),
        ),
        ("get", F::custom(resolve_get)),
        ("array_contains", F::custom(resolve_array_contains)),
        ("array_contains_all", F::custom(resolve_array_contains_all)),
        ("array_size", F::custom(resolve_array_size)),
        ("array_remove", F::custom(resolve_array_remove)),
        ("arrays_overlap", F::custom(resolve_arrays_overlap)),
        ("flatten", F::custom(resolve_flatten)),
        ("array_sort", F::custom(resolve_array_sort)),
    ]
}
