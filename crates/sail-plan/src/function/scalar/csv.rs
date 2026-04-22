use datafusion_expr::{Expr, ScalarUDF};
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::SparkSchemaOfCsv;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

fn from_csv(
    ScalarFunctionInput {
        arguments,
        function_context,
    }: ScalarFunctionInput,
) -> PlanResult<Expr> {
    let tz = function_context.plan_config.session_timezone.clone();
    let udf = ScalarUDF::from(SparkFromCSV::new(tz));
    Ok(udf.call(arguments))
}

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("from_csv", F::custom(from_csv)),
        ("schema_of_csv", F::udf(SparkSchemaOfCsv::new())),
        ("to_csv", F::unknown("to_csv")),
    ]
}
