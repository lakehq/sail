use datafusion::common::Result;
use datafusion::logical_expr::expr_rewriter::normalize_col;
use datafusion::logical_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion::logical_expr::{Expr, LogicalPlan};

pub(crate) fn rewrite_wildcard(
    input: LogicalPlan,
    expr: Vec<Expr>,
) -> Result<(LogicalPlan, Vec<Expr>)> {
    let schema = input.schema();
    let mut projected = vec![];
    for e in expr {
        match e {
            Expr::Wildcard { qualifier: None } => {
                projected.extend(expand_wildcard(schema, &input, None)?)
            }
            Expr::Wildcard {
                qualifier: Some(qualifier),
            } => projected.extend(expand_qualified_wildcard(&qualifier, schema, None)?),
            _ => projected.push(columnize_expr(normalize_col(e, &input)?, &input)?),
        }
    }
    Ok((input, projected))
}
