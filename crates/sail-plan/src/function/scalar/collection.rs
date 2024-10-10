use datafusion::functions::expr_fn as core_expr_fn;
use datafusion::functions_nested::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::extension::function::size::Size;
use crate::function::common::{Function, FunctionContext};

fn concat(args: Vec<expr::Expr>, _function_context: &FunctionContext) -> PlanResult<expr::Expr> {
    match args.first() {
        None
        | Some(expr::Expr::Literal(ScalarValue::Utf8(_)))
        | Some(expr::Expr::Literal(ScalarValue::Utf8View(_)))
        | Some(expr::Expr::Literal(ScalarValue::LargeUtf8(_))) => Ok(core_expr_fn::concat(args)),
        _ => Ok(expr_fn::array_concat(args)),
        // FIXME: Create UDF for concat to properly determine datatype
    }
}

fn reverse(arg: expr::Expr) -> expr::Expr {
    match arg {
        expr::Expr::Literal(ScalarValue::Utf8(_))
        | expr::Expr::Literal(ScalarValue::Utf8View(_))
        | expr::Expr::Literal(ScalarValue::LargeUtf8(_)) => core_expr_fn::reverse(arg),
        _ => expr_fn::array_reverse(arg),
        // FIXME: Create UDF for reverse to properly determine datatype
    }
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("array_size", F::unary(expr_fn::cardinality)),
        ("cardinality", F::udf(Size::new())),
        ("size", F::udf(Size::new())),
        ("concat", F::custom(concat)),
        ("reverse", F::unary(reverse)),
    ]
}
