use crate::function::common::Function;

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("coalesce", F::unknown("coalesce")),
        ("if", F::unknown("if")),
        ("ifnull", F::unknown("ifnull")),
        ("nanvl", F::unknown("nanvl")),
        ("nullif", F::unknown("nullif")),
        ("nvl", F::unknown("nvl")),
        ("nvl2", F::unknown("nvl2")),
        ("when", F::unknown("when")),
    ]
}
