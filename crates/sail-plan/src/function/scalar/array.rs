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
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_min_max::{ArrayMax, ArrayMin};
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn array_repeat(element: expr::Expr, count: expr::Expr) -> expr::Expr {
    expr_fn::array_repeat(element, cast(count, DataType::Int64))
}

fn array_compact(array: expr::Expr) -> expr::Expr {
    expr_fn::array_remove_all(array, lit(ScalarValue::Null))
}

fn slice(array: expr::Expr, start: expr::Expr, length: expr::Expr) -> expr::Expr {
    let start = cast(start, DataType::Int64);
    let length = cast(length, DataType::Int64);
    let end = start.clone() + length - lit(1_i64);
    expr_fn::array_slice(array, start, end, None)
}

fn sort_array(array: expr::Expr, asc: expr::Expr) -> PlanResult<expr::Expr> {
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

fn array_append(array: expr::Expr, element: expr::Expr) -> PlanResult<expr::Expr> {
    Ok(when(
        array.clone().is_not_null(),
        expr_fn::array_append(array, element),
    )
    .end()?)
}

fn array_prepend(array: expr::Expr, element: expr::Expr) -> PlanResult<expr::Expr> {
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

fn array_insert(
    array: expr::Expr,
    position: expr::Expr,
    value: expr::Expr,
) -> PlanResult<expr::Expr> {
    let array_len = cast(expr_fn::array_length(array.clone()), DataType::Int64);

    let pos_from_zero = when(position.clone().gt(lit(0)), position.clone() - lit(1))
        .when(
            position.clone().lt(lit(0)),
            array_len.clone() + position + lit(1),
        )
        .end()?;

    let zero_index_error = ScalarUDF::from(RaiseError::new()).call(vec![lit(
        "array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"
    )]);

    Ok(when(array.clone().is_null(), array.clone())
        .when(pos_from_zero.clone().is_null(), zero_index_error)
        .when(
            pos_from_zero.clone().lt(lit(0)),
            expr_fn::array_concat(vec![
                expr_fn::array_repeat(value.clone(), lit(1)),
                expr_fn::array_repeat(lit(ScalarValue::Null), -pos_from_zero.clone()),
                array.clone(),
            ]),
        )
        .when(
            pos_from_zero.clone().eq(lit(0)),
            expr_fn::array_prepend(value.clone(), array.clone()),
        )
        .when(
            pos_from_zero
                .clone()
                .between(lit(1), array_len.clone() - lit(1)),
            expr_fn::array_concat(vec![
                expr_fn::array_slice(array.clone(), lit(1), pos_from_zero.clone(), None),
                expr_fn::array_repeat(value.clone(), lit(1)),
                expr_fn::array_slice(
                    array.clone(),
                    pos_from_zero.clone() + lit(1),
                    array_len.clone(),
                    None,
                ),
            ]),
        )
        .when(
            pos_from_zero.clone().eq(array_len.clone()),
            expr_fn::array_append(array.clone(), value.clone()),
        )
        .when(
            pos_from_zero.clone().gt(array_len.clone()),
            expr_fn::array_concat(vec![
                array.clone(),
                expr_fn::array_repeat(lit(ScalarValue::Null), pos_from_zero - array_len),
                expr_fn::array_repeat(value, lit(1)),
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

    let same_type_null_only_array = expr::Expr::Cast(expr::Cast {
        expr: Box::new(make_array(vec![lit(ScalarValue::Null)])),
        data_type: left.get_type(function_context.schema)?,
    });

    let left_has_null = expr_fn::array_has_any(left.clone(), same_type_null_only_array.clone());
    let right_has_null = expr_fn::array_has_any(left.clone(), same_type_null_only_array);

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

/// Convert an array type to its nullable equivalent (all nested fields become nullable)
fn make_nullable_array_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::List(std::sync::Arc::new(
                datafusion::arrow::datatypes::Field::new(field.name(), inner_type, true),
            ))
        }
        DataType::LargeList(field) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::LargeList(std::sync::Arc::new(
                datafusion::arrow::datatypes::Field::new(field.name(), inner_type, true),
            ))
        }
        DataType::FixedSizeList(field, size) => {
            let inner_type = make_nullable_array_type(field.data_type());
            DataType::FixedSizeList(
                std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                    field.name(),
                    inner_type,
                    true,
                )),
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
        ("array_append", F::binary(array_append)),
        ("array_compact", F::unary(array_compact)),
        ("array_contains", F::binary(array_contains)),
        ("array_contains_all", F::binary(array_contains_all)),
        ("array_distinct", F::unary(expr_fn::array_distinct)),
        ("array_except", F::binary(expr_fn::array_except)),
        ("array_insert", F::ternary(array_insert)),
        ("array_intersect", F::binary(expr_fn::array_intersect)),
        ("array_join", F::udf(ArrayToString::new())),
        ("array_max", F::udf(ArrayMax::new())),
        ("array_min", F::udf(ArrayMin::new())),
        ("array_position", F::binary(array_position)),
        ("array_prepend", F::binary(array_prepend)),
        ("array_remove", F::binary(expr_fn::array_remove_all)),
        ("array_repeat", F::binary(array_repeat)),
        (
            "array_size",
            F::unary(|array| cast(expr_fn::array_length(array), DataType::Int32)),
        ),
        ("array_union", F::binary(expr_fn::array_union)),
        ("arrays_overlap", F::custom(arrays_overlap)),
        ("arrays_zip", F::udf(ArraysZip::new())),
        ("flatten", F::custom(flatten)),
        ("get", F::binary(array_element)),
        ("sequence", F::udf(SparkSequence::new())),
        ("shuffle", F::unary(array_fn::shuffle)),
        ("slice", F::ternary(slice)),
        ("sort_array", F::binary(sort_array)),
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

    #[allow(clippy::unwrap_used, clippy::panic)]
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

        let start = lit(2);
        let length = lit(3);
        let expr = slice(col("l1"), start, length);

        let df_schema = DFSchema::try_from(batch.schema())?;
        let physical_expr = SessionContext::new().create_physical_expr(expr, &df_schema)?;

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
