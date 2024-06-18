use std::sync::Arc;

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, ScalarFunctionDefinition, ScalarUDF};

pub(crate) mod alias;
pub(crate) mod explode;
pub(crate) mod wildcard;
pub(crate) mod window;

fn expr_to_udf(expr: &Expr) -> Option<(&Arc<ScalarUDF>, &Vec<Expr>)> {
    match expr {
        Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(udf),
            args,
        }) => Some((udf, args)),
        _ => None,
    }
}
