use std::sync::Arc;

use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::{Expr, LogicalPlan, Subquery, ident};

/// Evaluates a scalar subquery that occurs several times in a projection or filter only once.
///
/// Spark evaluates each scalar subquery expression once per outer row, whereas DataFusion's
/// `ScalarSubqueryToJoin` builds one join per occurrence of the expression. A subquery whose
/// row count and value are both consumed (see `rewrite_bounded_scalar_subquery` in sail-plan)
/// would then be evaluated once per consumer, and a volatile ORDER BY in it could select a
/// different row for each of them. Projecting such a subquery below the node leaves a single
/// occurrence for `ScalarSubqueryToJoin`.
#[derive(Debug, Default)]
pub struct ShareScalarSubqueries;

impl OptimizerRule for ShareScalarSubqueries {
    fn name(&self) -> &str {
        "share_scalar_subqueries"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let exprs: Vec<&Expr> = match &plan {
            LogicalPlan::Projection(projection) => projection.expr.iter().collect(),
            LogicalPlan::Filter(filter) => vec![&filter.predicate],
            _ => return Ok(Transformed::no(plan)),
        };
        let mut counts: Vec<(&Subquery, usize)> = vec![];
        for expr in exprs {
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
            return Ok(Transformed::no(plan));
        }
        let name_preserver = NamePreserver::new(&plan);
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
        let project_shared = |input: &Arc<LogicalPlan>| -> Result<LogicalPlan> {
            let exprs =
                input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .chain(shared.iter().map(|(subquery, alias)| {
                        Expr::ScalarSubquery(subquery.clone()).alias(alias)
                    }))
                    .collect();
            Ok(LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::clone(input),
            )?))
        };
        match plan {
            LogicalPlan::Projection(projection) => {
                let input = project_shared(&projection.input)?;
                let exprs = projection
                    .expr
                    .into_iter()
                    .map(share)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(exprs, Arc::new(input))?,
                )))
            }
            LogicalPlan::Filter(filter) => {
                let columns = filter
                    .input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
                let input = project_shared(&filter.input)?;
                let filter = LogicalPlan::Filter(Filter::try_new(
                    share(filter.predicate)?,
                    Arc::new(input),
                )?);
                // The projected subqueries are not part of the filter's output.
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(columns, Arc::new(filter))?,
                )))
            }
            plan => Ok(Transformed::no(plan)),
        }
    }
}
