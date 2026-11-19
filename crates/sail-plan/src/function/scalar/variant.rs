// SPDX-License-Identifier: Apache-2.0

use datafusion_expr::ScalarUDF;
use sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf;
use sail_function::scalar::variant::spark_parse_json::SparkParseJson;
use sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf;
use sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::function::common::ScalarFunction;

fn variant_get(safe: bool) -> ScalarFunction {
    crate::function::common::ScalarFunctionBuilder::custom(move |input| {
        let timezone = input.function_context.plan_config.session_timezone_label();
        Ok(ScalarUDF::from(SparkVariantGet::new(safe, timezone)).call(input.arguments))
    })
}

pub(super) fn list_built_in_variant_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("is_valid_variant", F::unknown("is_valid_variant")),
        ("is_variant_null", F::udf(SparkIsVariantNullUdf::new())),
        ("parse_json", F::udf(SparkParseJson::new(false))),
        ("schema_of_variant", F::udf(SparkSchemaOfVariantUdf::new())),
        // schema_of_variant_agg is registered as an aggregate function
        ("to_variant_object", F::udf(SparkToVariantObjectUdf::new())),
        ("try_parse_json", F::udf(SparkParseJson::new(true))),
        ("try_variant_get", variant_get(true)),
        ("variant_get", variant_get(false)),
        ("variant_to_json", F::udf(SparkVariantToJsonUdf::new())),
    ]
}
