use crate::extension::function::explode::{Explode, ExplodeKind};
use crate::function::common::Function;

pub(super) fn list_built_in_generator_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("explode", F::udf(Explode::new(ExplodeKind::Explode))),
        (
            "explode_outer",
            F::udf(Explode::new(ExplodeKind::ExplodeOuter)),
        ),
        ("inline", F::unknown("inline")),
        ("inline_outer", F::unknown("inline_outer")),
        ("posexplode", F::udf(Explode::new(ExplodeKind::PosExplode))),
        (
            "posexplode_outer",
            F::udf(Explode::new(ExplodeKind::PosExplodeOuter)),
        ),
        ("stack", F::unknown("stack")),
    ]
}

pub fn get_outer_built_in_generator_functions(name: &str) -> &str {
    match name.to_lowercase().as_str() {
        "explode" => "explode_outer",
        "inline" => "inline_outer",
        "posexplode" => "posexplode_outer",
        _ => name,
    }
}
