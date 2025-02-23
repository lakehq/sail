use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::nvl;
use datafusion::functions_nested::expr_fn;
use datafusion::functions_nested::position::array_position_udf;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, BinaryExpr, Operator};
use datafusion_functions_nested::string::ArrayToString;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::array::spark_array::SparkArray;
use crate::extension::function::array::spark_array_min_max::{ArrayMax, ArrayMin};
use crate::extension::function::array::spark_sequence::SparkSequence;
use crate::function::common::{Function, FunctionInput};
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

fn sort_array(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (array, asc) = arguments.two()?;
    let (sort, nulls) = match asc {
        expr::Expr::Literal(ScalarValue::Boolean(Some(true))) => (
            lit(ScalarValue::Utf8(Some("ASC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS FIRST".to_string()))),
        ),
        expr::Expr::Literal(ScalarValue::Boolean(Some(false))) => (
            lit(ScalarValue::Utf8(Some("DESC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS LAST".to_string()))),
        ),
        _ => {
            return Err(PlanError::invalid(format!(
                "Invalid asc value for sort_array: {}",
                asc
            )))
        }
    };
    Ok(expr_fn::array_sort(array, sort, nulls))
}

fn array_prepend(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    expr_fn::array_prepend(element, array)
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
    nvl(
        expr_fn::array_has(array, element),
        lit(ScalarValue::Boolean(Some(false))),
    )
}

fn array_contains_all(array: expr::Expr, element: expr::Expr) -> expr::Expr {
    nvl(
        expr_fn::array_has_all(array, element),
        lit(ScalarValue::Boolean(Some(false))),
    )
}

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array", F::udf(SparkArray::new())),
        ("array_append", F::binary(expr_fn::array_append)),
        ("array_compact", F::unary(array_compact)),
        ("array_contains", F::binary(array_contains)),
        ("array_contains_all", F::binary(array_contains_all)),
        ("array_distinct", F::unary(expr_fn::array_distinct)),
        ("array_except", F::binary(expr_fn::array_except)),
        ("array_insert", F::unknown("array_insert")),
        ("array_intersect", F::binary(expr_fn::array_intersect)),
        ("array_join", F::udf(ArrayToString::new())),
        ("array_max", F::udf(ArrayMax::new())),
        ("array_min", F::udf(ArrayMin::new())),
        ("array_position", F::scalar_udf(array_position_udf)),
        ("array_prepend", F::binary(array_prepend)),
        ("array_remove", F::binary(expr_fn::array_remove_all)),
        ("array_repeat", F::binary(array_repeat)),
        ("array_union", F::binary(expr_fn::array_union)),
        ("arrays_overlap", F::binary(expr_fn::array_has_any)),
        ("arrays_zip", F::unknown("arrays_zip")),
        ("flatten", F::unary(expr_fn::flatten)),
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
