use std::sync::Arc;

use datafusion_expr::HigherOrderUDF;

pub(crate) fn get_higher_order_function(
    name: &str,
    config: &crate::config::PlanConfig,
) -> Option<Arc<HigherOrderUDF>> {
    use sail_function::scalar::array::spark_array_aggregate::SparkArrayAggregate;
    use sail_function::scalar::array::spark_array_exists::SparkArrayExists;
    use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
    use sail_function::scalar::array::spark_array_forall::SparkArrayForall;
    use sail_function::scalar::array::spark_array_sort::SparkArraySort;
    use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
    use sail_function::scalar::map::spark_map_filter::SparkMapFilter;
    use sail_function::scalar::map::spark_transform_keys::SparkTransformKeys;
    use sail_function::scalar::map::spark_transform_values::SparkTransformValues;
    Some(Arc::new(match name {
        "transform" => HigherOrderUDF::new_from_impl(SparkArrayTransform::new()),
        "filter" => HigherOrderUDF::new_from_impl(SparkArrayFilter::new()),
        "exists" => HigherOrderUDF::new_from_impl(SparkArrayExists::new()),
        "forall" => HigherOrderUDF::new_from_impl(SparkArrayForall::new()),
        "aggregate" | "reduce" => HigherOrderUDF::new_from_impl(SparkArrayAggregate::new()),
        "array_sort" => HigherOrderUDF::new_from_impl(SparkArraySort::new()),
        "map_filter" => HigherOrderUDF::new_from_impl(SparkMapFilter::new()),
        "transform_values" => HigherOrderUDF::new_from_impl(SparkTransformValues::new()),
        "transform_keys" => HigherOrderUDF::new_from_impl(SparkTransformKeys::new(
            config.map_key_dedup_policy == crate::config::MapKeyDedupPolicy::LastWin,
        )),
        _ => return None,
    }))
}

/// Higher-order functions resolved together with their lambda scopes.
pub(crate) fn list_built_in_higher_order_functions() -> Vec<&'static str> {
    vec![
        "aggregate",
        "array_sort",
        "exists",
        "filter",
        "forall",
        "map_filter",
        "map_zip_with",
        "reduce",
        "transform",
        "transform_keys",
        "transform_values",
        "zip_with",
    ]
}
