use datafusion_expr::{expr, Expr, ScalarUDF};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::spark_to_csv::SparkToCsv;
use sail_function::scalar::csv::SparkSchemaOfCsv;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

fn from_csv(
    ScalarFunctionInput {
        mut arguments,
        function_context,
    }: ScalarFunctionInput,
) -> PlanResult<Expr> {
    let tz = function_context.plan_config.session_timezone.clone();
    // Try to constant-fold the schema argument (index 1) if it's not already a literal.
    // This handles cases like `from_csv(col, schema_of_csv(value))` where the schema
    // is a constant expression that can be evaluated at planning time.
    if arguments.len() >= 2 && !matches!(&arguments[1], expr::Expr::Literal(_, _)) {
        let evaluator = LiteralEvaluator::new();
        if let Ok(scalar) = evaluator.evaluate(&arguments[1]) {
            arguments[1] = expr::Expr::Literal(scalar, None);
        }
    }
    let udf = ScalarUDF::from(SparkFromCSV::new(tz));
    Ok(udf.call(arguments))
}

fn to_csv(
    ScalarFunctionInput {
        arguments,
        function_context,
    }: ScalarFunctionInput,
) -> PlanResult<Expr> {
    // Pass the current session timezone to the `to_csv` UDF.
    // It is used to localize TIMESTAMP (LTZ) values when formatting to CSV,
    // following the same pattern as `from_csv`.
    let tz = function_context.plan_config.session_timezone.clone();
    let udf = ScalarUDF::from(SparkToCsv::new(tz));
    Ok(udf.call(arguments))
}

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("from_csv", F::custom(from_csv)),
        ("schema_of_csv", F::udf(SparkSchemaOfCsv::new())),
        ("to_csv", F::custom(to_csv)),
    ]
}
