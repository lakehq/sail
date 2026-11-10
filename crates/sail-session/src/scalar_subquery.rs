use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_physical_expr::projection::ProjectionExpr;
use sail_plan::scalar_subquery_key::ScalarSubqueryKey;

/// Makes equivalent uncorrelated scalar subqueries share one body.
///
/// DataFusion plans and executes each distinct scalar subquery once, using exact
/// plan equality. Sail gives each subquery fresh output names, so a CTE referenced
/// by several scalar subqueries, or the same scalar subquery written several times,
/// would otherwise be executed once per occurrence.
#[allow(clippy::allow_attributes, clippy::mutable_key_type)]
pub fn share_equivalent_scalar_subqueries(
    plan: LogicalPlan,
) -> datafusion_common::Result<LogicalPlan> {
    // This scan avoids plan transformation for queries that cannot share a subquery.
    let mut candidates = 0;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::ScalarSubquery(subquery) = expr
                    && subquery.outer_ref_columns.is_empty()
                {
                    candidates += 1;
                }
                Ok(if candidates < 2 {
                    TreeNodeRecursion::Continue
                } else {
                    TreeNodeRecursion::Stop
                })
            })
        })
    })?;
    if candidates < 2 {
        return Ok(plan);
    }
    let mut bodies = HashMap::new();
    plan.transform_up_with_subqueries(|node| {
        let names = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            // A grouping set cannot be aliased to preserve its name.
            if matches!(expr, Expr::GroupingSet(_)) || !expr.contains_scalar_subquery() {
                return Ok(Transformed::no(expr));
            }
            let name = names.save(&expr);
            let expr = expr.transform_up(|expr| {
                let Expr::ScalarSubquery(mut subquery) = expr else {
                    return Ok(Transformed::no(expr));
                };
                let Some(key) = ScalarSubqueryKey::try_new(&subquery)? else {
                    return Ok(Transformed::no(Expr::ScalarSubquery(subquery)));
                };
                let body = bodies
                    .entry(key)
                    .or_insert_with(|| Arc::clone(&subquery.subquery));
                if Arc::ptr_eq(body, &subquery.subquery) {
                    return Ok(Transformed::no(Expr::ScalarSubquery(subquery)));
                }
                subquery.subquery = Arc::clone(body);
                Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
            })?;
            Ok(expr.update_data(|expr| name.restore(expr)))
        })
    })
    .data()
}

#[expect(deprecated)]
pub fn ensure_scalar_subquery_nullability(
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(plan));
        };

        let mut changed = false;
        let expressions = projection
            .expr()
            .iter()
            .map(|projection_expr| {
                let transformed = Arc::clone(&projection_expr.expr).transform_up(|expression| {
                    let Some(scalar) = expression.downcast_ref::<ScalarSubqueryExpr>() else {
                        return Ok(Transformed::no(expression));
                    };
                    if scalar.nullable() {
                        return Ok(Transformed::no(expression));
                    }
                    Ok(Transformed::yes(Arc::new(ScalarSubqueryExpr::new(
                        scalar.data_type().clone(),
                        true,
                        scalar.index(),
                        scalar.results().clone(),
                    ))
                        as Arc<dyn PhysicalExpr>))
                })?;
                changed |= transformed.transformed;
                Ok(ProjectionExpr::new(
                    transformed.data,
                    projection_expr.alias.clone(),
                ))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        if changed {
            Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                expressions,
                Arc::clone(projection.input()),
            )?) as Arc<dyn ExecutionPlan>))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .data()
}
