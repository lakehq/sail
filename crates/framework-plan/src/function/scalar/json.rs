use crate::function::common::Function;

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("from_json", F::unknown("from_json")),
        ("get_json_object", F::unknown("get_json_object")),
        ("json_array_length", F::unknown("json_array_length")),
        ("json_object_keys", F::unknown("json_object_keys")),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::unknown("to_json")),
    ]
}
