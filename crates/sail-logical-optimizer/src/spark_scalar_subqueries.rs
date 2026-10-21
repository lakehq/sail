use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::{Expr, LogicalPlan, Subquery, ident, lit};
use sail_logical_plan::correlation::never_null_unplannable;

/// Spark semantics for scalar subquery expressions that DataFusion's own rules would plan
/// differently or not at all.
///
/// 1. A null check on a scalar subquery expression that can never be NULL but that DataFusion
///    cannot plan (see [`never_null_unplannable`]) folds to its constant. The rule runs after
///    `SimplifyExpressions`, so that the subquery has been analyzed and constant-folded first
///    (an invalid constant cast inside it raises then, as it did before scalar subqueries were
///    nullable), and before `ScalarSubqueryToJoin`, which would fail on the subquery.
/// 2. A scalar subquery that occurs several times in a projection or filter is evaluated
///    once. Spark evaluates each scalar subquery expression once per outer row, whereas
///    `ScalarSubqueryToJoin` builds one join per occurrence of the expression. A subquery
///    whose row count and value are both consumed (see `rewrite_bounded_scalar_subquery` in
///    sail-plan) would then be evaluated once per consumer, and a volatile ORDER BY in it
///    could select a different row for each of them. Projecting such a subquery below the
///    node leaves a single occurrence for `ScalarSubqueryToJoin`.
#[derive(Debug, Default)]
pub struct SparkScalarSubqueries;

impl OptimizerRule for SparkScalarSubqueries {
    fn name(&self) -> &str {
        "spark_scalar_subqueries"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let name_preserver = NamePreserver::new(&plan);
        let plan = plan.map_expressions(|expr| {
            let saved = name_preserver.save(&expr);
            let expr = expr.transform_down(|expr| {
                Ok(match expr {
                    Expr::IsNull(inner) if never_null_unplannable(&inner) => {
                        Transformed::yes(lit(false))
                    }
                    Expr::IsNotNull(inner) if never_null_unplannable(&inner) => {
                        Transformed::yes(lit(true))
                    }
                    expr => Transformed::no(expr),
                })
            })?;
            Ok(Transformed::new_transformed(
                saved.restore(expr.data),
                expr.transformed,
            ))
        })?;
        let folded = plan.transformed;
        let plan = plan.data;
        let (exprs, input) = match &plan {
            LogicalPlan::Projection(projection) => {
                (projection.expr.clone(), Arc::clone(&projection.input))
            }
            LogicalPlan::Filter(filter) => {
                (vec![filter.predicate.clone()], Arc::clone(&filter.input))
            }
            _ => return Ok(Transformed::new_transformed(plan, folded)),
        };
        let mut counts: Vec<(&Subquery, usize)> = vec![];
        for expr in &exprs {
            expr.apply(|expr| {
                if let Expr::ScalarSubquery(subquery) = expr {
                    match counts.iter_mut().find(|(seen, _)| *seen == subquery) {
                        Some((_, count)) => *count += 1,
                        None => counts.push((subquery, 1)),
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        let shared: Vec<(Subquery, String)> = counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(subquery, _)| {
                (
                    subquery.clone(),
                    config.alias_generator().next("__shared_sq"),
                )
            })
            .collect();
        if shared.is_empty() {
            return Ok(Transformed::new_transformed(plan, folded));
        }
        let share = |expr: Expr| -> Result<Expr> {
            let saved = name_preserver.save(&expr);
            let expr = expr
                .transform_down(|expr| match expr {
                    Expr::ScalarSubquery(subquery) => {
                        match shared.iter().find(|(shared, _)| *shared == subquery) {
                            Some((_, alias)) => Ok(Transformed::yes(ident(alias))),
                            None => Ok(Transformed::no(Expr::ScalarSubquery(subquery))),
                        }
                    }
                    expr => Ok(Transformed::no(expr)),
                })?
                .data;
            Ok(saved.restore(expr))
        };
        let columns = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let lower =
            LogicalPlan::Projection(Projection::try_new(
                columns
                    .iter()
                    .cloned()
                    .chain(shared.iter().map(|(subquery, alias)| {
                        Expr::ScalarSubquery(subquery.clone()).alias(alias)
                    }))
                    .collect(),
                input,
            )?);
        let exprs = exprs.into_iter().map(share).collect::<Result<Vec<_>>>()?;
        Ok(Transformed::yes(match plan {
            LogicalPlan::Filter(_) => {
                let filter = with_exprs(plan, exprs, Arc::new(lower))?;
                // The projected subqueries are not part of the filter's output.
                LogicalPlan::Projection(Projection::try_new(columns, Arc::new(filter))?)
            }
            plan => with_exprs(plan, exprs, Arc::new(lower))?,
        }))
    }
}

/// The projection or filter with its expressions replaced over `input`.
fn with_exprs(
    plan: LogicalPlan,
    mut exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Filter(_) => {
            LogicalPlan::Filter(Filter::try_new(exprs.swap_remove(0), input)?)
        }
        _ => LogicalPlan::Projection(Projection::try_new(exprs, input)?),
    })
}
