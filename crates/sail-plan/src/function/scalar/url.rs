use datafusion_spark::function::url::try_url_decode::TryUrlDecode;
use datafusion_spark::function::url::url_decode::UrlDecode;
use datafusion_spark::function::url::url_encode::UrlEncode;
use sail_function::scalar::url::parse_url::ParseUrl;
use sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl;

use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn with_string_argument(
    input: ScalarFunctionInput,
    build: impl FnOnce(Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr,
) -> crate::error::PlanResult<datafusion_expr::Expr> {
    super::string::with_ltz_string_arguments(input, [0], |arguments| Ok(build(arguments)))
}

pub(super) fn list_built_in_url_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("parse_url", F::udf(ParseUrl::new())),
        ("try_parse_url", F::udf(SparkTryParseUrl::new())),
        (
            "try_url_decode",
            F::custom(|input| {
                with_string_argument(input, |arguments| {
                    datafusion_expr::ScalarUDF::from(TryUrlDecode::new()).call(arguments)
                })
            }),
        ),
        (
            "url_decode",
            F::custom(|input| {
                with_string_argument(input, |arguments| {
                    datafusion_expr::ScalarUDF::from(UrlDecode::new()).call(arguments)
                })
            }),
        ),
        (
            "url_encode",
            F::custom(|input| {
                with_string_argument(input, |arguments| {
                    datafusion_expr::ScalarUDF::from(UrlEncode::new()).call(arguments)
                })
            }),
        ),
    ]
}
