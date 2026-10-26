use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_json_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        ("json_tuple", F::kernel("json_tuple")),
        (
            "schema_of_json",
            F::udf(sail_function::scalar::json::SparkSchemaOfJson::new()),
        ),
    ]
}
