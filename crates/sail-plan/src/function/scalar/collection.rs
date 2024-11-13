use datafusion::functions_nested::expr_fn;

use crate::extension::function::size::Size;
use crate::extension::function::spark_concat::SparkConcat;
use crate::extension::function::spark_reverse::SparkReverse;
use crate::function::common::Function;

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::udf(Size::new())),
        ("size", F::udf(Size::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
    ]
}
