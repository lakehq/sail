use datafusion::functions_nested::expr_fn;
use datafusion_expr::{expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::map::map_from_arrays::MapFromArrays;
use sail_function::scalar::map::map_from_entries::MapFromEntries;
use sail_function::scalar::map::str_to_map::StrToMap;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    if !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::InvalidArgument(format!(
            "map(k1, v1, k2, v2, ...): expect number of args to be multiple of 2, got {}",
            input.arguments.len()
        )));
    }

    let (keys, values) = input
        .arguments
        .chunks(2)
        .map(|key_value| (key_value[0].clone(), key_value[1].clone()))
        .unzip();

    let keys = expr_fn::make_array(keys);
    let values = expr_fn::make_array(values);
    F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })
}

fn map_concat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let (keys, values) = input
        .arguments
        .iter()
        .map(|map| {
            (
                expr_fn::map_keys(map.clone()),
                expr_fn::map_values(map.clone()),
            )
        })
        .unzip();

    let keys = expr_fn::array_concat(keys);
    let values = expr_fn::array_concat(values);
    F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })
}

fn map_contains_key(map: expr::Expr, key: expr::Expr) -> expr::Expr {
    expr_fn::array_has(expr_fn::map_keys(map), key)
}

fn str_to_map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let (strs, delims) = input.arguments.at_least_one()?;

    let pair_delims = delims.first().cloned().unwrap_or(lit(","));
    let key_value_delims = delims.get(1).cloned().unwrap_or(lit(":"));

    F::udf(StrToMap::new())(ScalarFunctionInput {
        arguments: vec![strs, pair_delims, key_value_delims],
        function_context: input.function_context,
    })
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("map", F::custom(map)),
        ("map_concat", F::custom(map_concat)),
        ("map_contains_key", F::binary(map_contains_key)),
        ("map_entries", F::unary(expr_fn::map_entries)),
        ("map_from_arrays", F::udf(MapFromArrays::new())),
        ("map_from_entries", F::udf(MapFromEntries::new())),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::unary(expr_fn::map_values)),
        ("str_to_map", F::custom(str_to_map)),
    ]
}
