// SPDX-License-Identifier: Apache-2.0

use sail_function::scalar::variant::is_variant_null::IsVariantNullUdf;
use sail_function::scalar::variant::parse_json::ParseJson;
use sail_function::scalar::variant::schema_of_variant::SchemaOfVariantUdf;
use sail_function::scalar::variant::to_variant_object::ToVariantObjectUdf;
use sail_function::scalar::variant::variant_get::VariantGet;
use sail_function::scalar::variant::variant_to_json::VariantToJsonUdf;

use crate::function::common::ScalarFunction;

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
        ("try_variant_get", F::udf(VariantGet::new(true))),
        ("variant_get", F::udf(VariantGet::new(false))),
        ("variant_to_json", F::udf(VariantToJsonUdf::new())),
    ]
}
