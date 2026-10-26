use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_url_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "parse_url",
            F::udf(sail_function::scalar::url::parse_url::ParseUrl::new()),
        ),
        (
            "try_parse_url",
            F::udf(sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl::new()),
        ),
        (
            "try_url_decode",
            F::udf(datafusion_spark::function::url::try_url_decode::TryUrlDecode::new()),
        ),
        (
            "url_decode",
            F::udf(datafusion_spark::function::url::url_decode::UrlDecode::new()),
        ),
        (
            "url_encode",
            F::udf(datafusion_spark::function::url::url_encode::UrlEncode::new()),
        ),
    ]
}
