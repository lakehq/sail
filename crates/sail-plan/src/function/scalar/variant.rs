use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_variant_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
("is_valid_variant", F::kernel("is_valid_variant")),
("is_variant_null", F::udf(
            sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf::new(),
        )),
("parse_json", F::udf(
            sail_function::scalar::variant::spark_parse_json::SparkParseJson::new(false),
        )),
("schema_of_variant", F::udf(
            sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf::new(),
        )),
("to_variant_object", F::udf(
            sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf::new(),
        )),
("try_parse_json", F::udf(
            sail_function::scalar::variant::spark_parse_json::SparkParseJson::new(true),
        )),
("try_variant_get", F::udf(
            sail_function::scalar::variant::spark_variant_get::SparkVariantGet::new(true),
        )),
("variant_get", F::udf(
            sail_function::scalar::variant::spark_variant_get::SparkVariantGet::new(false),
        )),
("variant_to_json", F::udf(
            sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf::new(),
        )),
]
}
