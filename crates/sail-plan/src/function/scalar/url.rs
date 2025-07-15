use crate::extension::function::url::parse_url::ParseUrl;
use crate::extension::function::url::url_decode::UrlDecode;
use crate::extension::function::url::url_encode::UrlEncode;
use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_url_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("parse_url", F::udf(ParseUrl::new())),
        ("url_decode", F::udf(UrlDecode::new())),
        ("url_encode", F::udf(UrlEncode::new())),
    ]
}
