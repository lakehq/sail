use datafusion::functions_nested::expr_fn;

use crate::extension::function::collection::spark_concat::SparkConcat;
use crate::extension::function::collection::spark_reverse::SparkReverse;
use crate::extension::function::collection::spark_size::SparkSize;
use crate::function::common::ScalarFunction;

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::udf(SparkSize::new())),
        ("size", F::udf(SparkSize::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
    ]
}
