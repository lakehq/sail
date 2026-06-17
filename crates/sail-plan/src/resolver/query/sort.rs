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
        let LogicalPlan::Projection(Projection { input, expr, .. }) = plan else {
            return Ok(sorts);
        };
        let aggregate = Self::input_aggregate(input.as_ref());
        let sorts = sorts
            .iter()
            .map(|x| Self::rebase_sort_to_projection_input(x.clone(), expr))
            .collect::<PlanResult<Vec<_>>>()?;
        let sorts = if let Some(aggregate) = aggregate {
            sorts
                .into_iter()
                .map(|sort| Self::rewrite_sort_grouping_expr(sort, aggregate))
                .collect::<PlanResult<Vec<_>>>()?
        } else {
            sorts
        };
        let sorts = sorts
            .iter()
            .map(|x| Self::rebase_sort_to_projection_output(x.clone(), expr))
            .collect::<PlanResult<Vec<_>>>()?;

        if let Some(aggregate) = aggregate {
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

    fn input_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => Some(aggregate),
            LogicalPlan::Window(Window { input, .. }) => match input.as_ref() {
                LogicalPlan::Aggregate(aggregate) => Some(aggregate),
                _ => None,
            },
            _ => None,
        }
    }

    fn rewrite_sort_grouping_expr(sort: Sort, aggregate: &Aggregate) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let has_grouping_set = Self::has_grouping_set(&aggregate.group_expr);
        let grouping_exprs = Self::distinct_grouping_expressions_from_exprs(&aggregate.group_expr);
        let expr = Self::rewrite_grouping_expr(expr, &grouping_exprs, has_grouping_set)?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort_to_projection_input(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
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

    fn rebase_sort_to_projection_output(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let find = |target: &Expr| -> Option<Expr> {
            projection.iter().find_map(|expr| {
                if let Expr::Alias(Alias {
                    expr,
                    relation,
                    name,
                    ..
                }) = expr
                {
                    if expr.as_ref() == target {
                        return Some(Expr::Column(Column::new(relation.clone(), name.clone())));
                    }
                }
                None
            })
        };
        let expr = expr
            .transform_down(|e| {
                if let Some(expr) = find(&e) {
                    return Ok(Transformed::yes(expr));
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
