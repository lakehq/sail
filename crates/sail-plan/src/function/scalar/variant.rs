use sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf;
use sail_function::scalar::variant::spark_json_to_variant::SparkJsonToVariantUdf;
use sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf;
use sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_variant_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("is_variant_null", F::udf(SparkIsVariantNullUdf::new())),
        ("parse_json", F::udf(SparkJsonToVariantUdf::new())),
        ("schema_of_variant", F::udf(SparkSchemaOfVariantUdf::new())),
        ("schema_of_variant_agg", F::unknown("schema_of_variant_agg")),
        ("to_variant_object", F::udf(SparkToVariantObjectUdf::new())),
        ("try_parse_json", F::unknown("try_parse_json")),
        ("try_variant_get", F::udf(SparkVariantGet::new(true))),
        ("variant_explode", F::unknown("variant_explode")),
        ("variant_explode_outer", F::unknown("variant_explode_outer")),
        ("variant_get", F::udf(SparkVariantGet::new(false))),
        ("variant_to_json", F::udf(SparkVariantToJsonUdf::new())),
    ]
}
