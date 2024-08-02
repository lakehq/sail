use crate::function::common::Function;

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("from_csv", F::unknown("from_csv")),
        ("schema_of_csv", F::unknown("schema_of_csv")),
        ("to_csv", F::unknown("to_csv")),
    ]
}
