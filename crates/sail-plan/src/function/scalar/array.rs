use std::sync::Arc;

use arrow_schema::{DataType, FieldRef, TimeUnit};
use datafusion::functions::{core, string};
use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, Operator, ScalarUDF, ScalarUDFImpl};
use datafusion_functions_nested::string::ArrayToString;
use sail_common::utils::string::SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::array_intersect::ArrayIntersect;
use sail_function::scalar::array::array_position::SparkArrayPosition;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::{SparkSequence, SparkSequenceLazy};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn array_repeat(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (element, count) = values.two()?;
    let count = context.cast(count, DataType::Int64)?;
    let element_copy = context.copy_expression(&element)?;
    let count_copy = context.copy_expression(&count)?;
    let prototype = context.scalar_udf(
        functions_nested::repeat::array_repeat_udf(),
        vec![element_copy, count_copy],
    )?;
    let target = make_nullable_array_type(
        &prototype
            .result_type
            .resolved("array_repeat result")?
            .data_type,
    );
    array_repeat_with_nullable_element(context, element, count, target)
}

pub(crate) fn array_repeat_with_nullable_element(
    context: &mut FunctionContextInput<'_>,
    element: Expr,
    count: Expr,
    target: DataType,
) -> PlanResult<Expr> {
    let element_type = make_nullable_array_type(
        &element
            .result_type
            .resolved("array repeat element")?
            .data_type,
    );
    let element = context.cast(element, element_type)?;
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    let element = context.case(vec![(yes, element)], None)?;
    let result = context.scalar_udf(
        functions_nested::repeat::array_repeat_udf(),
        vec![element, count],
    )?;
    context.cast(result, target)
}

pub(crate) fn array_compact(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
) -> PlanResult<Expr> {
    context.scalar_udf(
        Arc::new(ScalarUDF::from(SparkArrayCompact::new())),
        vec![value],
    )
}

fn arrays_zip(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let names = input.argument_display_names;

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
    context.scalar_udf(Arc::new(ScalarUDF::from(ArraysZip::new(fields))), values)
}

fn slice(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (array, start, length) = values.three()?;
    let output_type = array.result_type.resolved("slice input")?.data_type.clone();
    let array = cast_list_value_nullability(context, array, true)?;
    let start = context.cast(start, DataType::Int64)?;
    let length = context.cast(length, DataType::Int64)?;
    let start_copy = context.copy_expression(&start)?;
    let end = context.binary(start_copy, Operator::Plus, length)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let end = context.binary(end, Operator::Minus, one)?;
    let result = context.scalar_udf(
        functions_nested::extract::array_slice_udf(),
        vec![array, start, end],
    )?;
    context.cast(result, output_type)
}

fn sort_array(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

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
    context.scalar_udf(
        functions_nested::sort::array_sort_udf(),
        vec![array, direction, nulls],
    )
}

fn array_append(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (array, element) = values.two()?;
    let copy = context.copy_expression(&array)?;
    let condition = context.is_not_null(copy);
    let array = cast_list_value_nullability(context, array, true)?;
    let value = if name == "array_append" {
        context.scalar_udf(
            functions_nested::concat::array_append_udf(),
            vec![array, element],
        )?
    } else {
        context.scalar_udf(
            functions_nested::concat::array_prepend_udf(),
            vec![element, array],
        )?
    };
    let value = cast_list_value_nullability(context, value, true)?;
    context.case(vec![(condition, value)], None)
}

fn array_element(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (array, element) = values.two()?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let element = context.binary(element, Operator::Plus, one)?;
    context.scalar_udf(
        functions_nested::extract::array_element_udf(),
        vec![array, element],
    )
}

fn array_contains(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (array, value) = values.two()?;
    let copy = context.copy_expression(&array)?;
    let result = context.scalar_udf(
        functions_nested::array_has::array_has_udf(),
        vec![copy, value],
    )?;
    let present = context.is_not_null(array);
    let no = context.literal(ScalarValue::Boolean(Some(false)));
    let fallback = context.case(vec![(present, no)], None)?;
    context.scalar_udf(core::coalesce(), vec![result, fallback])
}

fn array_contains_all(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (array, value) = values.two()?;
    let result = context.scalar_udf(
        functions_nested::array_has::array_has_all_udf(),
        vec![array, value],
    )?;
    let no = context.literal(ScalarValue::Boolean(Some(false)));
    context.scalar_udf(core::coalesce(), vec![result, no])
}

fn array_insert(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let (array, position, value) = input.arguments.three()?;

    let copy = context.copy_expression(&array)?;
    let array_input = cast_list_value_nullability(context, copy, true)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let prototype = context.scalar_udf(
        functions_nested::concat::array_append_udf(),
        vec![copy, value_copy],
    )?;
    let target = with_list_value_nullability(
        &prototype
            .result_type
            .resolved("array insert result")?
            .data_type,
        true,
    );
    let array_input = context.cast(array_input, target.clone())?;
    let copy = context.copy_expression(&array)?;
    let length = context.scalar_udf(functions_nested::length::array_length_udf(), vec![copy])?;
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
    let first = array_repeat_with_nullable_element(context, copy, one, target.clone())?;
    let copy = context.copy_expression(&index)?;
    let zero = context.literal(ScalarValue::Int64(Some(0)));
    let padding_length = context.binary(zero, Operator::Minus, copy)?;
    let null = context.literal(ScalarValue::Null);
    let padding =
        array_repeat_with_nullable_element(context, null, padding_length, target.clone())?;
    let copy = context.copy_expression(&array_input)?;
    let result = context.scalar_udf(
        functions_nested::concat::array_concat_udf(),
        vec![first, padding, copy],
    )?;
    branches.push((before, result));

    let copy = context.copy_expression(&index)?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    let at_start = context.binary(copy, Operator::Eq, zero)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let result = context.scalar_udf(
        functions_nested::concat::array_prepend_udf(),
        vec![value_copy, copy],
    )?;
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
    let prefix = context.scalar_udf(
        functions_nested::extract::array_slice_udf(),
        vec![copy, one, end],
    )?;
    let copy = context.copy_expression(&value)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let inserted = array_repeat_with_nullable_element(context, copy, one, target.clone())?;
    let copy = context.copy_expression(&index)?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let start = context.binary(copy, Operator::Plus, one)?;
    let copy = context.copy_expression(&array_input)?;
    let end = context.copy_expression(&length)?;
    let suffix = context.scalar_udf(
        functions_nested::extract::array_slice_udf(),
        vec![copy, start, end],
    )?;
    let result = context.scalar_udf(
        functions_nested::concat::array_concat_udf(),
        vec![prefix, inserted, suffix],
    )?;
    branches.push((middle, result));

    let copy = context.copy_expression(&index)?;
    let end = context.copy_expression(&length)?;
    let at_end = context.binary(copy, Operator::Eq, end)?;
    let copy = context.copy_expression(&array_input)?;
    let value_copy = context.copy_expression(&value)?;
    let result = context.scalar_udf(
        functions_nested::concat::array_append_udf(),
        vec![copy, value_copy],
    )?;
    branches.push((at_end, result));

    let copy = context.copy_expression(&index)?;
    let end = context.copy_expression(&length)?;
    let beyond = context.binary(copy, Operator::Gt, end)?;
    let padding_length = context.binary(index, Operator::Minus, length)?;
    let null = context.literal(ScalarValue::Null);
    let padding =
        array_repeat_with_nullable_element(context, null, padding_length, target.clone())?;
    let one = context.literal(ScalarValue::Int64(Some(1)));
    let last = array_repeat_with_nullable_element(context, value, one, target)?;
    let result = context.scalar_udf(
        functions_nested::concat::array_concat_udf(),
        vec![array_input, padding, last],
    )?;
    branches.push((beyond, result));
    context.case(branches, None)
}

fn arrays_overlap(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (left, right) = values.two()?;
    let target = with_list_value_nullability(
        &left.result_type.resolved("array overlap left")?.data_type,
        true,
    );
    let left = context.cast(left, target.clone())?;
    let right = context.cast(right, target.clone())?;
    let null = context.literal(ScalarValue::Null);
    let singleton =
        context.scalar_udf(functions_nested::make_array::make_array_udf(), vec![null])?;
    let singleton = context.cast(singleton, target)?;
    let singleton_copy = context.copy_expression(&singleton)?;
    let copy = context.copy_expression(&left)?;
    let left_nulls = context.scalar_udf(
        functions_nested::array_has::array_has_any_udf(),
        vec![copy, singleton_copy],
    )?;
    let copy = context.copy_expression(&right)?;
    let right_nulls = context.scalar_udf(
        functions_nested::array_has::array_has_any_udf(),
        vec![copy, singleton],
    )?;
    let has_nulls = context.binary(left_nulls, Operator::Or, right_nulls)?;
    let copy = context.copy_expression(&left)?;
    let left_null = context.is_null(copy);
    let copy = context.copy_expression(&right)?;
    let right_null = context.is_null(copy);
    let either_null = context.binary(left_null, Operator::Or, right_null)?;
    let copy = context.copy_expression(&left)?;
    let compact_left = array_compact(context, copy)?;
    let copy = context.copy_expression(&right)?;
    let compact_right = array_compact(context, copy)?;
    let overlap = context.scalar_udf(
        functions_nested::array_has::array_has_any_udf(),
        vec![compact_left, compact_right],
    )?;
    let null = context.literal(ScalarValue::Null);
    let null_overlap = context.binary(overlap, Operator::Or, null)?;
    let overlap = context.scalar_udf(
        functions_nested::array_has::array_has_any_udf(),
        vec![left, right],
    )?;
    let null = context.literal(ScalarValue::Null);
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    context.case(
        vec![
            (either_null, null),
            (has_nulls, null_overlap),
            (yes, overlap),
        ],
        None,
    )
}

fn flatten(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let array = values.one()?;
    let target = make_nullable_array_type(&array.result_type.resolved("flatten input")?.data_type);
    let array = context.cast(array, target.clone())?;
    let null = context.literal(ScalarValue::Null);
    let singleton =
        context.scalar_udf(functions_nested::make_array::make_array_udf(), vec![null])?;
    let singleton = context.cast(singleton, target)?;
    let copy = context.copy_expression(&array)?;
    let has_null = context.scalar_udf(
        functions_nested::array_has::array_has_any_udf(),
        vec![copy, singleton],
    )?;
    let no_null = context.not(has_null)?;
    let flattened = context.scalar_udf(functions_nested::flatten::flatten_udf(), vec![array])?;
    context.case(vec![(no_null, flattened)], None)
}

fn sequence(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let arguments = input.arguments;

    if !matches!(arguments.len(), 2 | 3) {
        return Err(PlanError::invalid(format!(
            "sequence requires 2 or 3 arguments, got {}",
            arguments.len()
        )));
    }
    let types = arguments
        .iter()
        .map(|a| {
            Ok(a.result_type
                .resolved("sequence argument")?
                .data_type
                .clone())
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let config = &context.config;
    let udf = SparkSequence::new(config.session_timezone.clone(), config.ansi_mode);
    let targets = udf.coerce_types(&types)?;
    let arguments = arguments
        .into_iter()
        .zip(types)
        .zip(targets)
        .map(|((argument, source), target)| {
            if source == target {
                return Ok(argument);
            }
            match (&source, &target) {
                (source, target) if source.is_string() && target.is_integer() => {
                    let spaces = context.literal(ScalarValue::Utf8(Some(
                        SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS.into(),
                    )));
                    let argument = context.scalar_udf(string::btrim(), vec![argument, spaces])?;
                    context.cast(argument, DataType::Int64)
                }
                (source, DataType::Date32) if source.is_string() => {
                    let spaces = context.literal(ScalarValue::Utf8(Some(
                        SPARK_WHITESPACE_OR_ISO_CONTROL_CHARACTERS.into(),
                    )));
                    let argument = context.scalar_udf(string::btrim(), vec![argument, spaces])?;
                    context.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkDate::new(false))),
                        vec![argument],
                    )
                }
                (source, DataType::Timestamp(TimeUnit::Microsecond, timezone))
                    if source.is_string() =>
                {
                    let function = ScalarUDF::from(SparkTimestamp::try_new(
                        timezone.clone(),
                        context.config.ansi_mode,
                        false,
                    )?);
                    context.scalar_udf(Arc::new(function), vec![argument])
                }
                (
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                ) => {
                    let ntz =
                        context.cast(argument, DataType::Timestamp(TimeUnit::Microsecond, None))?;
                    if crate::function::is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
                        return context.cast(ntz, target);
                    }
                    let source = context.literal(ScalarValue::Utf8(Some(timezone.to_string())));
                    let utc = context.literal(ScalarValue::Utf8(Some("UTC".into())));
                    let value = context.scalar_udf(
                        Arc::new(ScalarUDF::from(ConvertTz::new(false))),
                        vec![source, utc, ntz],
                    )?;
                    let value = context.cast(value, DataType::Int64)?;
                    context.cast(value, target)
                }
                _ => context.cast(argument, target),
            }
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let mut has_python = false;
    for argument in &arguments {
        has_python |= argument.any(&|node| match node {
            ExprKind::ScalarFunction { function, .. } => {
                function
                    .inner()
                    .is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>()
            }
            _ => false,
        })?;
    }
    if has_python {
        return context.scalar_udf(Arc::new(ScalarUDF::from(udf)), arguments);
    }
    crate::function::scalar::lambda::lazy_arguments(
        context,
        Arc::new(HigherOrderUDF::new_from_impl(SparkSequenceLazy::new(udf))),
        arguments,
    )
}

fn resolve_array_size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let result = context.scalar_udf(
        functions_nested::length::array_length_udf(),
        vec![values.one()?],
    )?;
    context.cast(result, DataType::Int32)
}

fn resolve_array_remove(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (a, b) = values.two()?;
    context.scalar_udf(functions_nested::remove::array_remove_all_udf(), vec![a, b])
}

pub(crate) fn cast_list_value_nullability(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    nullable: bool,
) -> PlanResult<Expr> {
    let target = with_list_value_nullability(
        &value.result_type.resolved("list input")?.data_type,
        nullable,
    );
    context.cast(value, target)
}

fn with_list_value_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::List(field) => {
            DataType::List(Arc::new(field.as_ref().clone().with_nullable(nullable)))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(Arc::new(field.as_ref().clone().with_nullable(nullable)))
        }
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(field.as_ref().clone().with_nullable(nullable)),
            *size,
        ),
        _ => data_type.clone(),
    }
}

fn make_nullable_array_type(data_type: &DataType) -> DataType {
    fn field(field: &FieldRef) -> FieldRef {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_nullable(true)
                .with_data_type(make_nullable_array_type(field.data_type())),
        )
    }
    match data_type {
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        _ => data_type.clone(),
    }
}

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array", F::udf(SparkArray::new())),
        (
            "array_append",
            F::custom(|input| array_append(input, "array_append")),
        ),
        (
            "array_compact",
            F::custom(|mut input| array_compact(&mut input.context, input.arguments.one()?)),
        ),
        ("array_contains", F::custom(array_contains)),
        ("array_contains_all", F::custom(array_contains_all)),
        (
            "array_distinct",
            F::scalar_udf(functions_nested::set_ops::array_distinct_udf),
        ),
        (
            "array_except",
            F::scalar_udf(functions_nested::except::array_except_udf),
        ),
        ("array_insert", F::custom(array_insert)),
        ("array_intersect", F::udf(ArrayIntersect::new())),
        ("array_join", F::udf(ArrayToString::new())),
        ("array_max", F::udf(ArrayMax::new())),
        ("array_min", F::udf(ArrayMin::new())),
        ("array_position", F::udf(SparkArrayPosition::new())),
        (
            "array_prepend",
            F::custom(|input| array_append(input, "array_prepend")),
        ),
        ("array_remove", F::custom(resolve_array_remove)),
        ("array_repeat", F::custom(array_repeat)),
        ("array_size", F::custom(resolve_array_size)),
        (
            "array_union",
            F::scalar_udf(functions_nested::set_ops::array_union_udf),
        ),
        ("arrays_overlap", F::custom(arrays_overlap)),
        ("arrays_zip", F::custom(arrays_zip)),
        ("flatten", F::custom(flatten)),
        ("get", F::custom(array_element)),
        ("sequence", F::custom(sequence)),
        (
            "shuffle",
            F::scalar_udf(datafusion_spark::function::array::shuffle),
        ),
        ("slice", F::custom(slice)),
        ("sort_array", F::custom(sort_array)),
    ]
}
