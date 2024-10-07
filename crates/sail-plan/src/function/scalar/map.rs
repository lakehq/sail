use datafusion::functions_nested::expr_fn;
use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::extension::function::map_function::MapFunction;
use crate::function::common::{Function, FunctionContext};
use crate::utils::ItemTaker;

fn map_contains_key(
    args: Vec<expr::Expr>,
    _function_context: &FunctionContext,
) -> PlanResult<expr::Expr> {
    let (map, key) = args.two()?;
    Ok(expr::Expr::Not(Box::new(expr_fn::array_empty(
        expr_fn::map_extract(map, key),
    ))))
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("element_at", F::binary(expr_fn::map_extract)), // TODO: element_at accepts array argument.
        ("map", F::udf(MapFunction::new())),
        ("map_concat", F::unknown("map_concat")),
        ("map_contains_key", F::custom(map_contains_key)),
        ("map_entries", F::var_arg(expr_fn::make_array)),
        ("map_from_arrays", F::unknown("map_from_arrays")),
        ("map_from_entries", F::unknown("map_from_entries")),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::unary(expr_fn::map_values)),
        ("str_to_map", F::unknown("str_to_map")),
        ("try_element_at", F::unknown("try_element_at")),
    ]
}
