use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_csv_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![(
        "schema_of_csv",
        F::udf(sail_function::scalar::csv::SparkSchemaOfCsv::new()),
    )]
}
