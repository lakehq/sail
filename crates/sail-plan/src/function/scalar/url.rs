use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_url_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("parse_url", F::unknown("parse_url")),
        ("url_decode", F::unknown("url_decode")),
        ("url_encode", F::unknown("url_encode")),
    ]
}
