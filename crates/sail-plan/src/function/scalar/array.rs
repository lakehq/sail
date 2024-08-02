use arrow::datatypes::DataType;
use datafusion::functions_array::expr_fn;
use datafusion_expr::expr;

use crate::function::common::Function;

fn array_repeat(element: expr::Expr, count: expr::Expr) -> expr::Expr {
    let count = expr::Expr::Cast(expr::Cast {
        expr: Box::new(count),
        data_type: DataType::Int64,
    });
    expr_fn::array_repeat(element, count)
}

pub(super) fn list_built_in_array_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array", F::var_arg(expr_fn::make_array)),
        ("array_append", F::binary(expr_fn::array_append)),
        ("array_compact", F::unknown("array_compact")),
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
        ("slice", F::unknown("slice")),
        ("sort_array", F::unknown("sort_array")),
    ]
}
