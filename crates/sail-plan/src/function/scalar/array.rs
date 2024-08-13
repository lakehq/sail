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
        lit(ScalarValue::Boolean(Some(true))) => (
            lit(ScalarValue::Utf8(Some("ASC".to_string()))),
            lit(ScalarValue::Utf8(Some("NULLS FIRST".to_string()))),
        ),
        lit(ScalarValue::Boolean(Some(false))) => (
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
