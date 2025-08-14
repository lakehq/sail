use datafusion::functions_nested::expr_fn;
use datafusion_expr::expr;
use datafusion_functions_nested::map::map_udf;

use crate::extension::function::map::map_function::MapFunction;
use crate::extension::function::map::spark_element_at::{SparkElementAt, SparkTryElementAt};
use crate::function::common::ScalarFunction;

fn map_contains_key(map: expr::Expr, key: expr::Expr) -> expr::Expr {
    expr_fn::array_has(expr_fn::map_keys(map), key)
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("element_at", F::udf(SparkElementAt::new())),
        ("map", F::udf(MapFunction::new())),
        ("map_concat", F::unknown("map_concat")),
        ("map_contains_key", F::binary(map_contains_key)),
        ("map_entries", F::unknown("map_entries")),
        ("map_from_arrays", F::scalar_udf(map_udf)),
        ("map_from_entries", F::unknown("map_from_entries")),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::unary(expr_fn::map_values)),
        ("str_to_map", F::unknown("str_to_map")),
        ("try_element_at", F::udf(SparkTryElementAt::new())),
    ]
}
