use datafusion::functions_nested::expr_fn;
use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::extension::function::size::Size;
use crate::function::common::Function;

fn concat(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    // FIXME: Concat accepts non-array arguments as well:
    //  "Concatenates multiple input columns together into a single column.
    //  The function works with strings, numeric, binary and compatible array columns."
    Ok(expr_fn::array_concat(args))
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::udf(Size::new())),
        ("size", F::udf(Size::new())),
        ("concat", F::custom(concat)),
        ("reverse", F::unary(expr_fn::array_reverse)),
    ]
}
