use datafusion::functions_nested::expr_fn;
use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::extension::function::map::map_function::MapFunction;
use crate::extension::function::map::spark_element_at::{SparkElementAt, SparkTryElementAt};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let keys = input
        .arguments
        .chunks(2)
        .map(|key_value| key_value[0].clone())
        .collect();

    let values = input
        .arguments
        .chunks(2)
        .map(|key_value| key_value[1].clone())
        .collect();

    let keys = expr_fn::make_array(keys);
    let values = expr_fn::make_array(values);
    F::udf(MapFunction::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })
}

fn map_contains_key(map: expr::Expr, key: expr::Expr) -> expr::Expr {
    expr_fn::array_has(expr_fn::map_keys(map), key)
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("element_at", F::udf(SparkElementAt::new())),
        ("map", F::custom(map)),
        ("map_concat", F::unknown("map_concat")),
        ("map_contains_key", F::binary(map_contains_key)),
        ("map_entries", F::unary(expr_fn::map_entries)),
        ("map_from_arrays", F::udf(MapFunction::new())),
        ("map_from_entries", F::udf(MapFunction::new())),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::unary(expr_fn::map_values)),
        ("str_to_map", F::unknown("str_to_map")),
        ("try_element_at", F::udf(SparkTryElementAt::new())),
    ]
}
