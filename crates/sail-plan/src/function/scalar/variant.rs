// SPDX-License-Identifier: Apache-2.0

use datafusion_expr::ScalarUDF;
use sail_function::scalar::variant::is_variant_null::IsVariantNullUdf;
use sail_function::scalar::variant::parse_json::ParseJson;
use sail_function::scalar::variant::schema_of_variant::SchemaOfVariantUdf;
use sail_function::scalar::variant::to_variant_object::ToVariantObjectUdf;
use sail_function::scalar::variant::variant_get::VariantGet;
use sail_function::scalar::variant::variant_to_json::VariantToJsonUdf;

use crate::function::common::ScalarFunction;

fn variant_get(safe: bool) -> ScalarFunction {
    crate::function::common::ScalarFunctionBuilder::custom(move |input| {
        let timezone = input.function_context.plan_config.session_timezone_label();
        Ok(ScalarUDF::from(VariantGet::new(safe, timezone)).call(input.arguments))
    })
}

pub(super) fn list_built_in_variant_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("is_valid_variant", F::unknown("is_valid_variant")),
        ("is_variant_null", F::udf(IsVariantNullUdf::new())),
        ("parse_json", F::udf(ParseJson::new(false))),
        ("schema_of_variant", F::udf(SchemaOfVariantUdf::new())),
        // schema_of_variant_agg is registered as an aggregate function
        ("to_variant_object", F::udf(ToVariantObjectUdf::new())),
        ("try_parse_json", F::udf(ParseJson::new(true))),
        ("try_variant_get", variant_get(true)),
        ("variant_get", variant_get(false)),
        ("variant_to_json", F::udf(VariantToJsonUdf::new())),
    ]
}
