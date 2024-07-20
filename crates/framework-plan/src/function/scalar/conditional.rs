use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::function::common::Function;

fn case(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let mut when_then_expr = Vec::new();
    let mut iter = args.into_iter();
    let mut else_expr: Option<Box<expr::Expr>> = None;
    while let Some(condition) = iter.next() {
        if let Some(result) = iter.next() {
            when_then_expr.push((Box::new(condition), Box::new(result)));
        } else {
            else_expr = Some(Box::new(condition));
            break;
        }
    }
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr,
    }))
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("coalesce", F::unknown("coalesce")),
        ("if", F::unknown("if")),
        ("ifnull", F::unknown("ifnull")),
        ("nanvl", F::unknown("nanvl")),
        ("nullif", F::unknown("nullif")),
        ("nvl", F::unknown("nvl")),
        ("nvl2", F::unknown("nvl2")),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}
