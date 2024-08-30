use std::sync::Arc;

use datafusion::functions_nested::expr_fn;
use datafusion_expr::{expr, ScalarUDF};

use crate::error::PlanResult;
use crate::extension::function::concat::ArrayConcat;
use crate::function::common::Function;

fn concat(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    // FIXME: Concat accepts non-array arguments as well:
    //  "Concatenates multiple input columns together into a single column.
    //  The function works with strings, numeric, binary and compatible array columns."
    Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(ArrayConcat::new())),
        args,
    }))
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
        ("concat", F::custom(concat)),
        ("reverse", F::unary(expr_fn::array_reverse)),
    ]
}
