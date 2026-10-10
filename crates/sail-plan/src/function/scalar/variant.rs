use arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{Expr, ScalarUDF, lit};
use sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf;
use sail_function::scalar::variant::spark_parse_json::SparkParseJson;
use sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf;
use sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::config::DefaultTimestampType;
use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

pub(super) fn list_built_in_variant_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("is_valid_variant", F::unknown("is_valid_variant")),
        ("is_variant_null", F::udf(SparkIsVariantNullUdf::new())),
        ("parse_json", F::udf(SparkParseJson::new(false))),
        ("schema_of_variant", F::udf(SparkSchemaOfVariantUdf::new())),
        // schema_of_variant_agg is registered as an aggregate function
        ("to_variant_object", F::udf(SparkToVariantObjectUdf::new())),
        ("try_parse_json", F::udf(SparkParseJson::new(true))),
        (
            "try_variant_get",
            F::custom(|input| variant_get(input, true)),
        ),
        ("variant_get", F::custom(|input| variant_get(input, false))),
        ("variant_to_json", F::udf(SparkVariantToJsonUdf::new())),
    ]
}

fn variant_get(input: ScalarFunctionInput, safe: bool) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if let Some(Expr::Literal(value, _)) = arguments.get(2)
        && let Some(name) = value.try_as_str().flatten()
    {
        let config = function_context.plan_config;
        let timezone = match name.trim().to_ascii_lowercase().as_str() {
            "timestamp" if config.default_timestamp_type == DefaultTimestampType::TimestampNtz => {
                Some(None)
            }
            "timestamp" | "timestamp_ltz" => Some(Some(config.session_timezone.clone())),
            "timestamp_ntz" => Some(None),
            _ => None,
        };
        if let Some(timezone) = timezone {
            // Resolve SQL timestamp names in the session before passing the
            // target to the runtime extractor, just as explicit CAST does.
            arguments[2] = lit(DataType::Timestamp(TimeUnit::Microsecond, timezone).to_string());
        }
    }
    Ok(ScalarUDF::from(SparkVariantGet::new(safe)).call(arguments))
}
