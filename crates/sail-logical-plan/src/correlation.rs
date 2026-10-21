use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::Case;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, Operator, Subquery};
use sail_function::scalar::misc::spark_scalar_subquery::SparkScalarSubquery;

use crate::row_bounds::row_bounds;

/// An expression that refers to the outer query only, with no inner column.
pub fn is_outer_only(expr: &Expr) -> bool {
    expr.contains_outer() && !expr.any_column_refs()
}

/// Whether DataFusion can decorrelate the subquery plan: every outer reference sits in a
/// filter conjunct that compares an inner expression with an outer one for equality. A
/// non-equality comparison, an OR, or an outer reference in any other node makes
/// `ScalarSubqueryToJoin` fail to plan the subquery.
pub fn decorrelates(plan: &LogicalPlan) -> bool {
    let mut decorrelates = true;
    let _ = plan.apply(|node| {
        decorrelates = match node {
            LogicalPlan::Filter(filter) => split_conjunction(&filter.predicate)
                .into_iter()
                .all(|conjunct| !conjunct.contains_outer() || is_correlated_equality(conjunct)),
            node => !node.contains_outer_reference(),
        };
        Ok(if decorrelates {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    decorrelates
}

fn is_correlated_equality(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            (is_outer_only(left) && !right.contains_outer())
                || (is_outer_only(right) && !left.contains_outer())
        }
        _ => false,
    }
}

/// Whether the subquery provably returns exactly one row whose value is never NULL.
fn single_non_null_row(subquery: &Subquery) -> bool {
    row_bounds(&subquery.subquery) == (1, Some(1))
        && !subquery.subquery.schema().field(0).is_nullable()
}

/// A resolved scalar subquery expression that can never be NULL but that DataFusion cannot
/// plan: a correlated subquery with a single non-null row that does not [`decorrelates`], or
/// the CASE over such subqueries that the resolver builds when it hoists outer-only guards
/// out of one (the wrapper around the CASE marks it as one scalar subquery expression). A
/// null check on it folds, as it did before scalar subqueries were nullable, since keeping
/// the check would fail planning. The subquery is then not evaluated, so an error it would
/// only raise at execution is not surfaced; an error raised while planning it has surfaced
/// already, because the fold runs after the subquery has been analyzed and simplified.
pub fn never_null_unplannable(expr: &Expr) -> bool {
    match SparkScalarSubquery::argument(expr) {
        Some(Expr::ScalarSubquery(subquery)) => {
            !subquery.outer_ref_columns.is_empty()
                && single_non_null_row(subquery)
                && !decorrelates(&subquery.subquery)
        }
        Some(Expr::Case(case)) => guard_case_never_null(case),
        _ => false,
    }
}

fn guard_case_never_null(case: &Case) -> bool {
    case.expr.is_none()
        && case
            .else_expr
            .as_deref()
            .is_some_and(guard_branch_never_null)
        && case
            .when_then_expr
            .iter()
            .all(|(_, then)| guard_branch_never_null(then))
}

fn guard_branch_never_null(expr: &Expr) -> bool {
    match expr {
        Expr::Case(case) => guard_case_never_null(case),
        expr => SparkScalarSubquery::subquery(expr).is_some_and(single_non_null_row),
    }
}
