use crate::extension::function::explode::Explode;
use crate::function::common::Function;

pub(super) fn list_built_in_generator_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("explode", F::udf(Explode::new("explode"))),
        ("explode_outer", F::udf(Explode::new("explode_outer"))),
        ("inline", F::unknown("inline")),
        ("inline_outer", F::unknown("inline_outer")),
        ("posexplode", F::udf(Explode::new("posexplode"))),
        ("posexplode_outer", F::udf(Explode::new("posexplode_outer"))),
        ("stack", F::unknown("stack")),
    ]
}
