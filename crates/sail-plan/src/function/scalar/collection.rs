use std::sync::Arc;

use datafusion::functions::expr_fn::concat_ws;
use datafusion::functions_nested::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, ScalarUDF};

use crate::extension::function::concat::ArrayConcat;
use crate::function::common::Function;

fn concat(array1: expr::Expr, array2: expr::Expr) -> expr::Expr {
    match (&array1, &array2) {
        (
            expr::Expr::Literal(ScalarValue::List(_))
            | expr::Expr::Literal(ScalarValue::LargeList(_)),
            expr::Expr::Literal(ScalarValue::List(_))
            | expr::Expr::Literal(ScalarValue::LargeList(_)),
        ) => expr::Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(ArrayConcat::new())),
            args: vec![array1, array2],
        }),
        _ => concat_ws(
            expr::Expr::Literal(ScalarValue::Utf8(Some("".to_string()))),
            vec![array1, array2],
        ),
    }
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
