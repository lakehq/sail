use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;

use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_csv", F::udf(SparkFromCSV::new())),
        ("schema_of_csv", F::unknown("schema_of_csv")),
        ("to_csv", F::unknown("to_csv")),
    ]
}
