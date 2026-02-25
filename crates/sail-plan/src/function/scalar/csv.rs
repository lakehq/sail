use datafusion_expr::ScalarUDF;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;

use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        (
            "from_csv",
            F::custom(
                |ScalarFunctionInput {
                     arguments,
                     function_context,
                 }| {
                    let tz = function_context.plan_config.session_timezone.clone();
                    let udf = ScalarUDF::from(SparkFromCSV::new(tz));
                    Ok(udf.call(arguments))
                },
            ),
        ),
        ("schema_of_csv", F::unknown("schema_of_csv")),
        ("to_csv", F::unknown("to_csv")),
    ]
}
