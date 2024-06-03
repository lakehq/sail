use crate::extension::function::map_function::MapFunction;
use crate::function::common::Function;

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("element_at", F::unknown("element_at")),
        ("map", F::udf(MapFunction::new())),
        ("map_concat", F::unknown("map_concat")),
        ("map_contains_key", F::unknown("map_contains_key")),
        ("map_entries", F::unknown("map_entries")),
        ("map_from_arrays", F::unknown("map_from_arrays")),
        ("map_from_entries", F::unknown("map_from_entries")),
        ("map_keys", F::unknown("map_keys")),
        ("map_values", F::unknown("map_values")),
        ("str_to_map", F::unknown("str_to_map")),
        ("try_element_at", F::unknown("try_element_at")),
    ]
}
