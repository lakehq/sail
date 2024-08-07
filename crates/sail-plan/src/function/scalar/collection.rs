use datafusion::functions_array::expr_fn;
use datafusion_expr::expr;

use crate::function::common::Function;

fn concat(array1: expr::Expr, array2: expr::Expr) -> expr::Expr {
    expr_fn::array_concat(vec![array1, array2])
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        // TODO: Write tests for `array_size`, `cardinality` `size` to validate that the behavior
        //  of expr_fn::cardinality fully matches behavior of Spark for all three.
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::unary(expr_fn::cardinality)),
        ("size", F::unary(expr_fn::cardinality)),
        ("concat", F::binary(concat)),
        ("reverse", F::unary(expr_fn::array_reverse)),
    ]
}
