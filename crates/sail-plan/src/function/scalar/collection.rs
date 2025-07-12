use crate::extension::function::collection::deep_size::DeepSize;
use crate::extension::function::collection::spark_concat::SparkConcat;
use crate::extension::function::collection::spark_reverse::SparkReverse;
use crate::extension::function::collection::spark_size::SparkSize;
use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array_size", F::udf(SparkSize::new(true, false))),
        // TODO: set second argument true
        // if spark.sql.ansi.enabled is false and spark.sql.legacy.sizeOfNull is true
        // https://spark.apache.org/docs/latest/api/sql/index.html#cardinality
        ("cardinality", F::udf(SparkSize::new(false, false))),
        ("deep_size", F::udf(DeepSize::new())),
        ("size", F::udf(SparkSize::new(false, false))),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
    ]
}
