use datafusion_expr::lit;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn array_sort(input: ScalarFunctionInput) -> PlanResult<datafusion_expr::Expr> {
    let (array, rest) = input.arguments.at_least_one()?;

    // Check if there's a second argument (lambda comparator)
    if !rest.is_empty() {
        // array_sort with lambda comparator is not yet implemented
        return Err(crate::error::PlanError::todo(
            "array_sort with lambda comparator is not yet implemented",
        ));
    }

    // array_sort(array) without lambda - sorts in ascending order
    super::array::sort_array(array, lit(true))
}

pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aggregate", F::unknown("aggregate")),
        ("array_sort", F::custom(array_sort)),
        ("exists", F::unknown("exists")),
        ("filter", F::unknown("filter")),
        ("forall", F::unknown("forall")),
        ("map_filter", F::unknown("map_filter")),
        ("map_zip_with", F::unknown("map_zip_with")),
        ("reduce", F::unknown("reduce")),
        ("transform", F::unknown("transform")),
        ("transform_keys", F::unknown("transform_keys")),
        ("transform_values", F::unknown("transform_values")),
        ("zip_with", F::unknown("zip_with")),
    ]
}
