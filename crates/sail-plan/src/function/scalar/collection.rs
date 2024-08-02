use crate::function::common::Function;

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unknown("array_size")),
        ("cardinality", F::unknown("cardinality")),
        ("concat", F::unknown("concat")),
        ("reverse", F::unknown("reverse")),
        ("size", F::unknown("size")),
    ]
}
