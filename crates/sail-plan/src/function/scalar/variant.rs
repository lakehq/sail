use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_variant_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("is_variant_null", F::unknown("is_variant_null")),
        ("parse_json", F::unknown("parse_json")),
        ("schema_of_variant", F::unknown("schema_of_variant")),
        ("schema_of_variant_agg", F::unknown("schema_of_variant_agg")),
        ("to_variant_object", F::unknown("to_variant_object")),
        ("try_parse_json", F::unknown("try_parse_json")),
        ("try_variant_get", F::unknown("try_variant_get")),
        ("variant_explode", F::unknown("variant_explode")),
        ("variant_explode_outer", F::unknown("variant_explode_outer")),
        ("variant_get", F::unknown("variant_get")),
    ]
}
