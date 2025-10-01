use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Column;
use datafusion_expr::expr::{Alias, Sort};
use datafusion_expr::{
    Aggregate, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Projection, Window,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::sort::SortWithinPartitionsNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_sort(
        &self,
        input: spec::QueryPlan,
        order: Vec<spec::SortOrder>,
        is_global: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let sorts = self
            .resolve_query_sort_orders_by_plan(&input, &order, state)
            .await?;
        let sorts = Self::rebase_query_sort_orders(sorts, &input)?;
        if is_global {
            Ok(LogicalPlanBuilder::from(input).sort(sorts)?.build()?)
        } else {
            // TODO: Use the logical plan builder to include logic such as expression rebase.
            //   We can build a plan with a `Sort` node and then replace it with the
            //   `SortWithinPartitions` node using a tree node rewriter.
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), sorts, None)),
            }))
        }
    }

    /// Rebase sort expressions using aggregation expressions when the aggregate plan
    /// is inside a projection plan.
    /// Usually the [LogicalPlanBuilder] handles rebase, but this particular case is not handled yet.
    /// We do not do so recursively to make sure this workaround is only applied to a particular pattern.
    ///
    /// This workaround is needed for queries where the aggregation expression is aliased.
    /// Here is an example.
    /// ```sql
    /// SELECT a, sum(b) AS s FROM VALUES (1, 2) AS t(a, b) GROUP BY a ORDER BY sum(b)
    /// ```
    fn rebase_query_sort_orders(sorts: Vec<Sort>, plan: &LogicalPlan) -> PlanResult<Vec<Sort>> {
        let candidate = match plan {
            LogicalPlan::Projection(Projection { input, expr, .. }) => {
                let sorts = sorts
                    .iter()
                    .map(|x| Self::rebase_sort_before_projection(x.clone(), expr))
                    .collect::<PlanResult<Vec<_>>>()?;

                if let LogicalPlan::Aggregate(aggregate) = input.as_ref() {
                    Some((aggregate, sorts))
                } else if let LogicalPlan::Window(Window { input, .. }) = input.as_ref() {
                    if let LogicalPlan::Aggregate(aggregate) = input.as_ref() {
                        Some((aggregate, sorts))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some((aggregate, sorts)) = candidate {
            let Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } = aggregate;
            let base = group_expr
                .iter()
                .cloned()
                .chain(aggr_expr.iter().cloned())
                .collect::<Vec<_>>();
            sorts
                .into_iter()
                .map(|x| Self::rebase_sort(x, &base, input.as_ref()))
                .collect::<PlanResult<Vec<_>>>()
        } else {
            Ok(sorts)
        }
    }

    fn rebase_sort_before_projection(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let find = |col: &Column| -> Option<Expr> {
            projection.iter().find_map(|expr| {
                if let Expr::Alias(Alias {
                    expr,
                    relation,
                    name,
                    ..
                }) = expr
                {
                    if relation == &col.relation && name == &col.name {
                        return Some(expr.as_ref().clone());
                    }
                }
                None
            })
        };
        let expr = expr
            .transform_down(|e| {
                if let Expr::Column(ref col) = e {
                    if let Some(expr) = find(col) {
                        return Ok(Transformed::yes(expr));
                    }
                }
                Ok(Transformed::no(e))
            })
            .data()?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort(sort: Sort, base: &[Expr], plan: &LogicalPlan) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let expr = Self::rebase_expression(expr, base, plan)?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    /// Resolve sort orders by attempting child plans recursively.
    async fn resolve_query_sort_orders_by_plan(
        &self,
        plan: &LogicalPlan,
        sorts: &[spec::SortOrder],
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Sort>> {
        let mut results: Vec<Sort> = Vec::with_capacity(sorts.len());
        for sort in sorts {
            let expr = self
                .resolve_query_sort_order_by_plan(plan, sort, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }

    /// Resolve a sort order by attempting child plans recursively.
    /// This is needed since the sort order may refer to a column in a child plan,
    /// So we need to use the schema of the child plan to map between user-facing
    /// field name and the opaque field ID.
    #[async_recursion]
    async fn resolve_query_sort_order_by_plan(
        &self,
        plan: &LogicalPlan,
        sort: &spec::SortOrder,
        state: &mut PlanResolverState,
    ) -> PlanResult<Sort> {
        let sort_expr = self
            .resolve_sort_order(sort.clone(), true, plan.schema(), state)
            .await;
        match sort_expr {
            Ok(sort_expr) => Ok(sort_expr),
            Err(_) => {
                let mut sorts = Vec::with_capacity(plan.inputs().len());
                for input_plan in plan.inputs() {
                    let sort_expr = self
                        .resolve_query_sort_order_by_plan(input_plan, sort, state)
                        .await?;
                    sorts.push(sort_expr);
                }
                if sorts.len() != 1 {
                    Err(PlanError::invalid(format!("sort expression: {sort:?}")))
                } else {
                    Ok(sorts.one()?)
                }
            }
        }
    }
}
