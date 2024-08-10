use datafusion::functions_array::expr_fn;
use datafusion_expr::expr;

use crate::function::common::Function;

fn concat(array1: expr::Expr, array2: expr::Expr) -> expr::Expr {
    expr_fn::array_concat(vec![array1, array2])
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        // FIXME: expr_fn::cardinality doesn't fully match expected behavior.
        //  Spark's cardinality function seems to be the same as the size function.
        //  `cardinality`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.cardinality.html
        //  `size`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.size.html
        ("cardinality", F::unary(expr_fn::cardinality)),
        ("size", F::unary(expr_fn::cardinality)),
        ("concat", F::binary(concat)),
        ("reverse", F::unary(expr_fn::array_reverse)),
    ]
}
