use datafusion_common::tree_node::TreeNode;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

/// An expression that refers to the outer query only, with no inner column and no nested
/// subquery (which may refer to inner columns of its own).
pub fn is_outer_only(expr: &Expr) -> bool {
    expr.contains_outer() && !expr.any_column_refs() && !contains_subquery(expr)
}

fn contains_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_)
                | Expr::InSubquery(_)
                | Expr::Exists(_)
                | Expr::SetComparison(_)
        ))
    })
    .unwrap_or(true)
}

/// Whether DataFusion can decorrelate the subquery plan: every outer reference sits in a
/// filter conjunct that compares an inner column expression with an outer expression for
/// equality, and only projections, filters and aggregates stand between the subquery's
/// output and such a filter. A non-equality comparison, an OR, a predicate on the outer row
/// alone, an outer reference in any other node, or a derived table, join, distinct, sort,
/// limit, union or unnest above the correlated filter makes `ScalarSubqueryToJoin` fail to
/// pull the correlation up (its `PullUpCorrelatedExpr` only propagates the correlated
/// columns through those three node kinds).
pub fn decorrelates(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => {
            split_conjunction(&filter.predicate)
                .into_iter()
                .all(|conjunct| !conjunct.contains_outer() || is_correlated_equality(conjunct))
                && decorrelates(&filter.input)
        }
        LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) => {
            !plan.contains_outer_reference() && plan.inputs().into_iter().all(decorrelates)
        }
        // Any other node may only stand over an uncorrelated subtree.
        _ => plan.all_out_ref_exprs().is_empty(),
    }
}

/// `inner = outer` (either way round), where the inner side refers to an inner column.
fn is_correlated_equality(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => (is_outer_only(left) && is_inner(right)) || (is_outer_only(right) && is_inner(left)),
        _ => false,
    }
}

fn is_inner(expr: &Expr) -> bool {
    expr.any_column_refs() && !expr.contains_outer() && !contains_subquery(expr)
}
