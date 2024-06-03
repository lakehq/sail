use crate::function::common::Function;

pub(super) fn list_built_in_url_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("parse_url", F::unknown("parse_url")),
        ("url_decode", F::unknown("url_decode")),
        ("url_encode", F::unknown("url_encode")),
    ]
}
