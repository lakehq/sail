use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::Column;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::{Alias, Sort};
use datafusion_expr::expr_rewriter::rewrite_sort_cols_by_aggs;
use datafusion_expr::{
    Aggregate, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Projection, Window,
};
use sail_common::spec;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::SortWithinPartitionsNode;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

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
        let (expressions, _) = self
            .resolve_missing_input_expressions(
                order.iter().cloned().map(spec::Expr::SortOrder).collect(),
                &input,
                true,
                state,
            )
            .await?;
        let sorts = expressions
            .into_iter()
            .zip(order)
            .map(|(expr, sort)| Self::sort_with_options(expr, sort.direction, sort.null_ordering))
            .collect();
        let sorts = Self::rebase_query_sort_orders(sorts, &input)?;
        let output_schema = Arc::clone(input.schema());
        let plan = if is_global {
            // The logical plan builder rewrites the sort orders in the same way.
            let sorts = rewrite_sort_cols_by_aggs(sorts, &input)?;
            let input = Self::add_sort_missing_inputs(input, &sorts, state)?;
            LogicalPlanBuilder::from(input).sort(sorts)?.build()?
        } else {
            // TODO: Use the logical plan builder to include logic such as expression rebase.
            //   We can build a plan with a `Sort` node and then replace it with the
            //   `SortWithinPartitions` node using a tree node rewriter.
            let input = Self::add_sort_missing_inputs(input, &sorts, state)?;
            LogicalPlan::Extension(Extension {
                node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), sorts, None)),
            })
        };
        Self::restore_missing_input_output(plan, output_schema)
    }

    /// Spark adds the attributes missing from sort orders to every operator between the
    /// sort and the descendant that outputs them. DataFusion's logical plan builder only
    /// extends one projection, so add them here when every operator in between can carry them.
    fn add_sort_missing_inputs(
        input: LogicalPlan,
        sorts: &[Sort],
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let columns = sorts
            .iter()
            .flat_map(|sort| sort.expr.column_refs())
            .collect::<HashSet<_>>();
        if columns
            .iter()
            .all(|column| input.schema().has_column(column))
        {
            return Ok(input);
        }
        // TODO: The physical optimizer pushes sorts below `MonotonicIdExec` and into limits
        //   (as a top-k sort), which changes the result, so do not recover more sort columns
        //   above them until it doesn't.
        if input.exists(|plan| {
            Ok(match plan {
                LogicalPlan::Limit(_) => true,
                LogicalPlan::Extension(extension) => {
                    extension.node.as_any().is::<MonotonicIdNode>()
                }
                _ => false,
            })
        })? {
            return Ok(input);
        }
        Ok(Self::add_missing_inputs(&input, &columns, state)?.unwrap_or(input))
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
                    && relation == &col.relation
                    && name == &col.name
                {
                    return Some(expr.as_ref().clone());
                }
                None
            })
        };
        let expr = expr
            .transform_down(|e| {
                if let Expr::Column(ref col) = e
                    && let Some(expr) = find(col)
                {
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
                    && expr.as_ref() == target
                {
                    return Some(Expr::Column(Column::new(relation.clone(), name.clone())));
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
}
