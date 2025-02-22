use datafusion::functions::expr_fn;
use datafusion_expr::expr;

use crate::error::PlanResult;
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

fn case(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let mut when_then_expr = Vec::new();
    let mut iter = arguments.into_iter();
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

fn if_expr(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("coalesce", F::var_arg(expr_fn::coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::binary(expr_fn::nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nvl", F::binary(expr_fn::nvl)),
        ("nvl2", F::ternary(expr_fn::nvl2)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}
