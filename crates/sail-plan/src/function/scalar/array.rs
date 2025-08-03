use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::{coalesce, nvl};
use datafusion::functions_nested::expr_fn;
use datafusion::functions_nested::position::array_position as datafusion_array_position;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    cast, expr, is_null, lit, not, or, when, BinaryExpr, ExprSchemable, Operator,
};
use datafusion_functions_nested::make_array::make_array;
use datafusion_functions_nested::string::ArrayToString;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::array::arrays_zip::ArraysZip;
use crate::extension::function::array::spark_array::SparkArray;
use crate::extension::function::array::spark_array_min_max::{ArrayMax, ArrayMin};
use crate::extension::function::array::spark_sequence::SparkSequence;
use crate::extension::function::raise_error::RaiseError;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::utils::ItemTaker;

fn array_repeat(element: expr::Expr, count: expr::Expr) -> expr::Expr {
    let count = expr::Expr::Cast(expr::Cast {
        expr: Box::new(count),
        data_type: DataType::Int64,
    });
    expr_fn::array_repeat(element, count)
}

fn array_compact(array: expr::Expr) -> expr::Expr {
    expr_fn::array_remove_all(array, lit(ScalarValue::Null))
}

fn slice(array: expr::Expr, start: expr::Expr, length: expr::Expr) -> expr::Expr {
    let start = expr::Expr::Cast(expr::Cast {
        expr: Box::new(start),
        data_type: DataType::Int64,
    });
    let length = expr::Expr::Cast(expr::Cast {
        expr: Box::new(length),
        data_type: DataType::Int64,
    });
    let end = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(start.clone()),
        op: Operator::Plus,
        right: Box::new(expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(length),
            op: Operator::Minus,
            right: Box::new(lit(ScalarValue::Int64(Some(1)))),
        })),
    });
    expr_fn::array_slice(array, start, end, None)
}

fn sort_array(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (array, asc) = arguments.two()?;
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

fn array_append(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(expr::Expr::IsNotNull(Box::new(array.clone()))),
            Box::new(expr_fn::array_append(array, element)),
        )],
        else_expr: None,
    })
}

fn array_prepend(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(expr::Expr::IsNotNull(Box::new(array.clone()))),
            Box::new(expr_fn::array_prepend(element, array)),
        )],
        else_expr: None,
    })
}

fn array_element(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    let element = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(element),
        op: Operator::Plus,
        right: Box::new(lit(ScalarValue::Int64(Some(1)))),
    });
    expr_fn::array_element(array, element)
}

fn array_contains(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    coalesce(vec![
        expr_fn::array_has(array.clone(), element),
        expr::Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(expr::Expr::IsNotNull(Box::new(array))),
                Box::new(lit(ScalarValue::Boolean(Some(false)))),
            )],
            else_expr: None,
        }),
    ])
}

fn array_contains_all(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    nvl(
        expr_fn::array_has_all(array, element),
        lit(ScalarValue::Boolean(Some(false))),
    )
}

fn array_position(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    coalesce(vec![
        datafusion_array_position(
            expr::Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(expr_fn::cardinality(array.clone())),
                        op: Operator::Gt,
                        right: Box::new(lit(ScalarValue::UInt64(Some(0)))),
                    })),
                    Box::new(array.clone()),
                )],
                else_expr: None, // datafusion panics if from_index > array_len
            }), // So if the inner array_len == 0, search in NULL array instead
            element,
            lit(ScalarValue::Int32(Some(1))),
        ),
        expr::Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(expr::Expr::IsNotNull(Box::new(array.clone()))),
                Box::new(lit(ScalarValue::Int32(Some(0)))),
            )], // if Array is not NULL, but elem not found, need to return 0
            else_expr: None, // On NULL array still return NULL
        }),
    ])
}

fn array_insert(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let (array, position, value) = input.arguments.three()?;

    let array_len = expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::array_length(array.clone())),
        data_type: DataType::Int64,
    });

    let pos_from_zero = when(position.clone().gt(lit(0)), position.clone() - lit(1))
        .when(
            position.clone().lt(lit(0)),
            array_len.clone() + position + lit(1),
        )
        .end()?;

    let zero_index_error = F::udf(RaiseError::new())(ScalarFunctionInput {
        arguments: vec![lit("[INVALID_INDEX_OF_ZERO] The index 0 is invalid. 
        An index shall be either < 0 or > 0 (the first element has index 1)")],
        function_context: input.function_context,
    })?;

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

    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![
            (
                Box::new(or(is_null(left.clone()), is_null(right.clone()))),
                Box::new(lit(ScalarValue::Null)),
            ),
            (
                Box::new(or(left_has_null, right_has_null)),
                Box::new(or(
                    expr_fn::array_has_any(
                        array_compact(left.clone()),
                        array_compact(right.clone()),
                    ),
                    lit(ScalarValue::Null),
                )),
            ),
        ],
        else_expr: Some(Box::new(expr_fn::array_has_any(left, right))),
    }))
}

fn flatten(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let array = arguments.one()?;

    let same_type_null_only_array = expr::Expr::Cast(expr::Cast {
        expr: Box::new(make_array(vec![lit(ScalarValue::Null)])),
        data_type: array.get_type(function_context.schema)?,
    });

    let array_has_null = expr_fn::array_has_any(array.clone(), same_type_null_only_array);

    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(not(array_has_null)),
            Box::new(expr_fn::flatten(array)),
        )],
        else_expr: None,
    }))
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
        ("array_insert", F::custom(array_insert)),
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
        ("shuffle", F::unknown("shuffle")),
        ("slice", F::ternary(slice)),
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
