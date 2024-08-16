use arrow::datatypes::DataType;
use datafusion::functions_array::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, BinaryExpr, Operator};

use crate::error::{PlanError, PlanResult};
use crate::function::common::Function;
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

fn sort_array(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let (array, asc) = args.two()?;
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

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array", F::var_arg(expr_fn::make_array)),
        ("array_append", F::binary(expr_fn::array_append)),
        ("array_compact", F::unary(array_compact)),
        ("array_contains", F::binary(expr_fn::array_has)),
        ("array_distinct", F::unary(expr_fn::array_distinct)),
        ("array_except", F::binary(expr_fn::array_except)),
        ("array_insert", F::unknown("array_insert")),
        ("array_intersect", F::binary(expr_fn::array_intersect)),
        ("array_join", F::unknown("array_join")),
        ("array_max", F::unknown("array_max")),
        ("array_min", F::unknown("array_min")),
        ("array_position", F::unknown("array_position")),
        ("array_prepend", F::binary(expr_fn::array_prepend)),
        ("array_remove", F::binary(expr_fn::array_remove_all)),
        ("array_repeat", F::binary(array_repeat)),
        ("array_union", F::binary(expr_fn::array_union)),
        ("arrays_overlap", F::unknown("arrays_overlap")),
        ("arrays_zip", F::unknown("arrays_zip")),
        ("flatten", F::unary(expr_fn::flatten)),
        ("get", F::binary(expr_fn::array_element)),
        ("sequence", F::ternary(expr_fn::gen_series)),
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
        let batch = RecordBatch::try_from_iter([("l1", Arc::new(l1) as _)]).unwrap();

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