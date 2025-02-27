use datafusion::functions_nested::expr_fn;
use datafusion_expr::expr;
use datafusion_functions_nested::map::map_udf;

use crate::error::PlanResult;
use crate::extension::function::map::map_function::MapFunction;
use crate::extension::function::map::spark_element_at::{SparkElementAt, SparkTryElementAt};
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

fn map_contains_key(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (map, key) = arguments.two()?;
    Ok(expr::Expr::Not(Box::new(expr_fn::array_empty(
        expr_fn::map_extract(map, key),
    ))))
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("element_at", F::udf(SparkElementAt::new())),
        ("map", F::udf(MapFunction::new())),
        ("map_concat", F::unknown("map_concat")),
        ("map_contains_key", F::custom(map_contains_key)),
        ("map_entries", F::unknown("map_entries")),
        ("map_from_arrays", F::scalar_udf(map_udf)),
        ("map_from_entries", F::unknown("map_from_entries")),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::unary(expr_fn::map_values)),
        ("str_to_map", F::unknown("str_to_map")),
        ("try_element_at", F::udf(SparkTryElementAt::new())),
    ]
}
