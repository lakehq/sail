use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::function::common::Function;

fn case(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let mut when_then_expr = Vec::new();
    let mut iter = args.into_iter();
    // FIXME: Add more validation on the number of arguments.
    while let (Some(condition), Some(result)) = (iter.next(), iter.next()) {
        when_then_expr.push((Box::new(condition), Box::new(result)));
    }
    let else_expr = iter.next().map(Box::new); // The last element, if any, is the else expression
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates this into the when_then_expr
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
