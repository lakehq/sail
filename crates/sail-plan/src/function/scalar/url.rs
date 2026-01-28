use datafusion_spark::function::url::try_url_decode::TryUrlDecode;
use datafusion_spark::function::url::url_decode::UrlDecode;
use datafusion_spark::function::url::url_encode::UrlEncode;
use sail_function::scalar::url::parse_url::ParseUrl;
use sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl;

use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_url_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("parse_url", F::udf(ParseUrl::new())),
        ("try_parse_url", F::udf(SparkTryParseUrl::new())),
        ("try_url_decode", F::udf(TryUrlDecode::new())),
        ("url_decode", F::udf(UrlDecode::new())),
        ("url_encode", F::udf(UrlEncode::new())),
    ]
}
