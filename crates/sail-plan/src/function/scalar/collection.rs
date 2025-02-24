use datafusion::functions_nested::expr_fn;

use crate::extension::function::collection::spark_concat::SparkConcat;
use crate::extension::function::collection::spark_reverse::SparkReverse;
use crate::extension::function::collection::spark_size::SparkSize;
use crate::function::common::Function;

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::udf(SparkSize::new())),
        ("size", F::udf(SparkSize::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
    ]
}
