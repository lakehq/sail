use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_lambda_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("aggregate", F::unknown("aggregate")),
        ("array_sort", F::unknown("array_sort")),
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
