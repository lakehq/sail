use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::{coalesce, nvl};
use datafusion::functions_nested::expr_fn;
use datafusion::functions_nested::position::array_position as datafusion_array_position;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, is_null, lit, not, or, when, ExprSchemable, ScalarUDF};
use datafusion_functions_nested::make_array::make_array;
use datafusion_functions_nested::string::ArrayToString;
use datafusion_spark::function::array::expr_fn as array_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::array_intersect::ArrayIntersect;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_compact::SparkArrayCompact;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn array_repeat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (element, count) = input.arguments.two()?;
    let count = cast(count, DataType::Int64);
    let output_type = make_nullable_array_type(
        &expr_fn::array_repeat(element.clone(), count.clone()).get_type(schema.as_ref())?,
    );
    array_repeat_with_nullable_element(element, count, output_type, schema)
}

fn array_repeat_with_nullable_element(
    element: expr::Expr,
    count: expr::Expr,
    output_type: DataType,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<expr::Expr> {
    let element_type = make_nullable_array_type(&element.get_type(schema.as_ref())?);
    let nullable_element = when(lit(true), cast(element, element_type)).end()?;
    Ok(cast(
        expr_fn::array_repeat(nullable_element, count),
        output_type,
    ))
}

fn array_compact(array: expr::Expr) -> expr::Expr {
    ScalarUDF::from(SparkArrayCompact::new()).call(vec![array])
}

fn arrays_zip(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| match expr {
            expr::Expr::Column(_) | expr::Expr::Alias(_) => name.clone(),
            _ => format!("{i}"),
        })
        .collect();
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(ArraysZip::new(field_names))),
        args: input.arguments,
    }))
}

fn slice(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, start, length) = input.arguments.three()?;
    slice_expr(array, start, length, schema)
}

fn slice_expr(
    array: expr::Expr,
    start: expr::Expr,
    length: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<expr::Expr> {
    let output_type = array.get_type(schema.as_ref())?;
    let array = cast_list_value_nullability(array, schema, true)?;
    let start = cast(start, DataType::Int64);
    let length = cast(length, DataType::Int64);
    let end = start.clone() + length - lit(1_i64);
    Ok(cast(
        expr_fn::array_slice(array, start, end, None),
        output_type,
    ))
}

fn sort_array(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (array, asc) = match input.arguments.len() {
        1 => (
            input.arguments.one()?,
            lit(ScalarValue::Boolean(Some(true))),
        ),
        2 => input.arguments.two()?,
        n => {
            return Err(PlanError::invalid(format!(
                "sort_array requires 1 or 2 arguments, got {n}"
            )))
        }
    };
    let (sort, nulls) = match asc {
        expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _metadata) => (
            lit(ScalarValue::Utf8(Some("ASC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS FIRST".to_string()))),
        ),
        expr::Expr::Literal(ScalarValue::Boolean(Some(false)), _metadata) => (
            lit(ScalarValue::Utf8(Some("DESC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS LAST".to_string()))),
        ),
        _ => {
            return Err(PlanError::invalid(format!(
                "Invalid asc value for sort_array: {asc}"
            )))
        }
    };
    Ok(expr_fn::array_sort(array, sort, nulls))
}

fn array_append(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, element) = input.arguments.two()?;
    let output_type = array_append_type(&array, &element, schema)?;
    let array_input = cast_list_value_nullability(array.clone(), schema, true)?;
    let appended = cast(expr_fn::array_append(array_input, element), output_type);
    Ok(when(array.clone().is_not_null(), appended).end()?)
}

fn array_prepend(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, element) = input.arguments.two()?;
    let array = cast_list_value_nullability(array, schema, true)?;
    Ok(when(
        array.clone().is_not_null(),
        expr_fn::array_prepend(element, array),
    )
    .end()?)
}

fn array_element(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    expr_fn::array_element(array, element + lit(1_i64))
}

fn array_contains(array: expr::Expr, element: expr::Expr) -> PlanResult<expr::Expr> {
    Ok(coalesce(vec![
        expr_fn::array_has(array.clone(), element),
        when(array.is_not_null(), lit(false)).end()?,
    ]))
}

fn array_contains_all(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    nvl(expr_fn::array_has_all(array, element), lit(false))
}

fn array_position(array: expr::Expr, element: expr::Expr) -> PlanResult<expr::Expr> {
    Ok(coalesce(vec![
        datafusion_array_position(
            // datafusion panics if from_index > array_len
            // So if the inner array_len == 0, search in NULL array instead
            when(
                expr_fn::cardinality(array.clone()).gt(lit(0)),
                array.clone(),
            )
            .end()?,
            element,
            lit(1_i32),
        ),
        when(array.clone().is_not_null(), lit(0_i32)).end()?,
    ]))
}

fn array_insert(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (array, position, value) = input.arguments.three()?;
    let output_type = with_list_value_nullability(&array.get_type(schema.as_ref())?, true);
    let array_input = cast_list_value_nullability(array.clone(), schema, true)?;
    let array_len = cast(expr_fn::array_length(array.clone()), DataType::Int64);

    let pos_from_zero = when(position.clone().gt(lit(0)), position.clone() - lit(1))
        .when(
            position.clone().lt(lit(0)),
            array_len.clone() + position + lit(1),
        )
        .end()?;

    let zero_index_error = cast(ScalarUDF::from(RaiseError::new()).call(vec![lit(
        "array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"
    )]), output_type.clone());

    Ok(when(
        array.clone().is_null(),
        cast(array.clone(), output_type.clone()),
    )
    .when(pos_from_zero.clone().is_null(), zero_index_error)
    .when(
        pos_from_zero.clone().lt(lit(0)),
        expr_fn::array_concat(vec![
            array_repeat_with_nullable_element(value.clone(), lit(1), output_type.clone(), schema)?,
            array_repeat_with_nullable_element(
                lit(ScalarValue::Null),
                -pos_from_zero.clone(),
                output_type.clone(),
                schema,
            )?,
            array_input.clone(),
        ]),
    )
    .when(
        pos_from_zero.clone().eq(lit(0)),
        expr_fn::array_prepend(value.clone(), array_input.clone()),
    )
    .when(
        pos_from_zero
            .clone()
            .between(lit(1), array_len.clone() - lit(1)),
        expr_fn::array_concat(vec![
            expr_fn::array_slice(array_input.clone(), lit(1), pos_from_zero.clone(), None),
            array_repeat_with_nullable_element(value.clone(), lit(1), output_type.clone(), schema)?,
            expr_fn::array_slice(
                array_input.clone(),
                pos_from_zero.clone() + lit(1),
                array_len.clone(),
                None,
            ),
        ]),
    )
    .when(
        pos_from_zero.clone().eq(array_len.clone()),
        expr_fn::array_append(array_input.clone(), value.clone()),
    )
    .when(
        pos_from_zero.clone().gt(array_len.clone()),
        expr_fn::array_concat(vec![
            array_input,
            array_repeat_with_nullable_element(
                lit(ScalarValue::Null),
                pos_from_zero - array_len,
                output_type.clone(),
                schema,
            )?,
            array_repeat_with_nullable_element(value, lit(1), output_type, schema)?,
        ]),
    )
    .end()?)
}

fn arrays_overlap(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (left, right) = arguments.two()?;

    let nullable_array_type =
        with_list_value_nullability(&left.get_type(function_context.schema.as_ref())?, true);
    let left = cast(left, nullable_array_type.clone());
    let right = cast(right, nullable_array_type.clone());
    let same_type_null_only_array = cast(
        make_array(vec![lit(ScalarValue::Null)]),
        nullable_array_type,
    );

    let left_has_null = expr_fn::array_has_any(left.clone(), same_type_null_only_array.clone());
    let right_has_null = expr_fn::array_has_any(right.clone(), same_type_null_only_array);

    Ok(when(
        or(is_null(left.clone()), is_null(right.clone())),
        lit(ScalarValue::Null),
    )
    .when(
        or(left_has_null, right_has_null),
        or(
            expr_fn::array_has_any(array_compact(left.clone()), array_compact(right.clone())),
            lit(ScalarValue::Null),
        ),
    )
    .when(lit(true), expr_fn::array_has_any(left, right))
    .end()?)
}

fn flatten(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let array = arguments.one()?;
    let array_type = array.get_type(function_context.schema)?;

    // Convert array type to nullable version to avoid Arrow errors with non-nullable empty arrays
    let nullable_array_type = make_nullable_array_type(&array_type);
    let nullable_array = cast(array.clone(), nullable_array_type.clone());

    // Cast the null-only array to the nullable type to match
    let same_type_null_only_array = cast(
        make_array(vec![lit(ScalarValue::Null)]),
        nullable_array_type,
    );

    let array_has_null = expr_fn::array_has_any(nullable_array.clone(), same_type_null_only_array);

    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(not(array_has_null)),
            Box::new(expr_fn::flatten(nullable_array)),
        )],
        else_expr: None,
    }))
}

fn array_append_type(
    array: &expr::Expr,
    element: &expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
) -> PlanResult<DataType> {
    let array_type = array.get_type(schema.as_ref())?;
    let contains_null = list_value_contains_null(&array_type).unwrap_or(true)
        || element.nullable(schema.as_ref())?;
    Ok(with_list_value_nullability(&array_type, contains_null))
}

fn cast_list_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    nullable: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let target_type = with_list_value_nullability(&data_type, nullable);
    if target_type == data_type {
        Ok(expr)
    } else {
        Ok(cast(expr, target_type))
    }
}

fn list_value_contains_null(data_type: &DataType) -> Option<bool> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => Some(field.is_nullable()),
        DataType::FixedSizeList(field, _) => Some(field.is_nullable()),
        _ => None,
    }
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

/// Convert an array type to its nullable equivalent (all nested fields become nullable)
fn make_nullable_array_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                inner_type,
                true,
            )))
        }
        DataType::LargeList(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::LargeList(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                inner_type,
                true,
            )))
        }
        DataType::FixedSizeList(field, size) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(field.name(), inner_type, true)),
                *size,
            )
        }
        other => other.clone(),
    }
}

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array", F::udf(SparkArray::new())),
        ("array_append", F::custom(array_append)),
        ("array_compact", F::unary(array_compact)),
        ("array_contains", F::binary(array_contains)),
        ("array_contains_all", F::binary(array_contains_all)),
        ("array_distinct", F::unary(expr_fn::array_distinct)),
        ("array_except", F::binary(expr_fn::array_except)),
        ("array_insert", F::custom(array_insert)),
        ("array_intersect", F::udf(ArrayIntersect::new())),
        ("array_join", F::udf(ArrayToString::new())),
        ("array_max", F::udf(ArrayMax::new())),
        ("array_min", F::udf(ArrayMin::new())),
        ("array_position", F::binary(array_position)),
        ("array_prepend", F::custom(array_prepend)),
        ("array_remove", F::binary(expr_fn::array_remove_all)),
        ("array_repeat", F::custom(array_repeat)),
        (
            "array_size",
            F::unary(|array| cast(expr_fn::array_length(array), DataType::Int32)),
        ),
        ("array_union", F::binary(expr_fn::array_union)),
        ("arrays_overlap", F::custom(arrays_overlap)),
        ("arrays_zip", F::custom(arrays_zip)),
        ("flatten", F::custom(flatten)),
        ("get", F::binary(array_element)),
        ("sequence", F::udf(SparkSequence::new())),
        ("shuffle", F::unary(array_fn::shuffle)),
        ("slice", F::custom(slice)),
        ("sort_array", F::custom(sort_array)),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ListArray, RecordBatch};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, ColumnarValue};

    use super::*;

    #[expect(clippy::unwrap_used, clippy::panic)]
    #[test]
    fn test_slice() -> PlanResult<()> {
        let l1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        let batch = RecordBatch::try_from_iter([("l1", Arc::new(l1) as _)])?;

        let df_schema = Arc::new(DFSchema::try_from(batch.schema())?);
        let expr = slice_expr(col("l1"), lit(2), lit(3), &df_schema)?;
        let physical_expr = SessionContext::new().create_physical_expr(expr, df_schema.as_ref())?;

        let result = physical_expr.evaluate(&batch)?;
        let result_array = match result {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<ListArray>().unwrap().clone()
            }
            _ => panic!("Expected an array result"),
        };

        let expected_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(2),
            Some(3),
            Some(4),
        ])]);

        assert_eq!(result_array, expected_array);

        Ok(())
    }
}
