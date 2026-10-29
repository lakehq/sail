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
        let output_schema = Arc::clone(input.schema());
        let resolved = self
            .resolve_missing_input_expressions(
                order.iter().cloned().map(spec::Expr::SortOrder).collect(),
                &input,
                true,
                state,
            )
            .await;
        let sorts = if resolved.is_err()
            && is_global
            && matches!(order.as_slice(), [sort]
            if matches!(sort.child.as_ref(), spec::Expr::UnresolvedAttribute { name, .. }
                if name.parts().len() == 1 && name.parts()[0].as_ref().eq_ignore_ascii_case("all")
                && self.resolve_column_candidates(input.schema(), name.parts()[0].as_ref(), None, state).is_empty()))
        {
            input
                .schema()
                .columns()
                .into_iter()
                .filter(|column| {
                    state
                        .get_field_info(column.name())
                        .is_ok_and(|info| !info.is_hidden())
                })
                .map(|column| {
                    Self::sort_with_options(
                        Expr::Column(column),
                        order[0].direction.clone(),
                        order[0].null_ordering.clone(),
                    )
                })
                .collect()
        } else {
            let (expressions, _) = resolved?;
            expressions
                .into_iter()
                .zip(order)
                .map(|(expr, sort)| {
                    Self::sort_with_options(expr, sort.direction, sort.null_ordering)
                })
                .collect()
        };
        let sorts = Self::rebase_query_sort_orders(sorts, &input)?;
        let (input, sorts) = Self::materialize_sort_aggregates(input, sorts, state)?;
        let (input, sorts) = if is_global {
            // Visible sort orders need no recovery, so leave their rewrite to the builder.
            // Rewriting scans the input projection for each sort order.
            if Self::missing_sort_columns(&input, &sorts).is_some() {
                // The logical plan builder rewrites the sort orders in the same way.
                let sorts = rewrite_sort_cols_by_aggs(sorts, &input)?;
                (Self::add_sort_missing_inputs(input, &sorts, state)?, sorts)
            } else {
                (input, sorts)
            }
        } else {
            let input = Self::add_sort_missing_inputs(input, &sorts, state)?;
            (input, sorts)
        };
        let (input, expressions) = self.rewrite_partition_expressions(
            input,
            sorts.iter().map(|sort| sort.expr.clone()).collect(),
            state,
        )?;
        let sorts = sorts
            .into_iter()
            .zip(expressions)
            .map(|(sort, expr)| Sort { expr, ..sort })
            .collect::<Vec<_>>();
        let plan = if is_global {
            LogicalPlanBuilder::from(input).sort(sorts)?.build()?
        } else {
            // TODO: Use the logical plan builder to include logic such as expression rebase.
            //   We can build a plan with a `Sort` node and then replace it with the
            //   `SortWithinPartitions` node using a tree node rewriter.
            LogicalPlan::Extension(Extension {
                node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), sorts, None)),
            })
        };
        Self::restore_missing_input_output(plan, output_schema)
    }

    /// Materialize aggregates mentioned only by ORDER BY, carrying their outputs
    /// through intervening projections/HAVING and restoring the visible schema above.
    fn materialize_sort_aggregates(
        input: LogicalPlan,
        sorts: Vec<Sort>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<Sort>)> {
        let aggregates = datafusion_expr::utils::find_aggregate_exprs(
            &sorts
                .iter()
                .map(|sort| sort.expr.clone())
                .collect::<Vec<_>>(),
        );
        if aggregates.is_empty() {
            return Ok((input, sorts));
        }
        fn extend(
            plan: LogicalPlan,
            aggregates: &[Expr],
            allow_new: bool,
            state: &mut PlanResolverState,
        ) -> PlanResult<(LogicalPlan, Vec<(Expr, Expr)>)> {
            match plan {
                LogicalPlan::Aggregate(aggregate) => {
                    let mut exprs = aggregate.aggr_expr.clone();
                    let mut replacements = vec![];
                    for expr in aggregates {
                        let rebound = PlanResolver::rebase_to_projection_input(
                            expr.clone(),
                            &PlanResolver::distinct_grouping_expressions_from_exprs(
                                &aggregate.group_expr,
                            ),
                        )?;
                        let column = if let Some(index) = exprs
                            .iter()
                            .position(|existing| existing.clone().unalias() == rebound)
                        {
                            Expr::Column(Column::from(
                                aggregate
                                    .schema
                                    .qualified_field(aggregate.group_expr_len()? + index),
                            ))
                        } else {
                            if !allow_new {
                                return Err(crate::error::PlanError::analysis(
                                    "cannot resolve new ORDER BY aggregate through HAVING",
                                ));
                            }
                            let name = state.register_field_name("");
                            exprs.push(rebound.alias(&name));
                            Expr::Column(Column::new_unqualified(name))
                        };
                        replacements.push((expr.clone(), column));
                    }
                    Ok((
                        LogicalPlan::Aggregate(Aggregate::try_new(
                            aggregate.input,
                            aggregate.group_expr,
                            exprs,
                        )?),
                        replacements,
                    ))
                }
                LogicalPlan::Projection(projection) => {
                    let rebound = aggregates
                        .iter()
                        .map(|expr| {
                            PlanResolver::rebase_to_projection_input(expr.clone(), &projection.expr)
                        })
                        .collect::<PlanResult<Vec<_>>>()?;
                    let (input, replacements) = extend(
                        Arc::unwrap_or_clone(projection.input),
                        &rebound,
                        allow_new,
                        state,
                    )?;
                    let replacements = aggregates
                        .iter()
                        .zip(replacements)
                        .map(|(original, (_, column))| (original.clone(), column))
                        .collect::<Vec<_>>();
                    let mut exprs = projection.expr;
                    for (_, column) in &replacements {
                        if !exprs.contains(column) {
                            exprs.push(column.clone());
                        }
                    }
                    Ok((
                        LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(input))?),
                        replacements,
                    ))
                }
                LogicalPlan::Filter(mut filter) => {
                    let (input, replacements) =
                        extend(Arc::unwrap_or_clone(filter.input), aggregates, false, state)?;
                    filter.input = Arc::new(input);
                    Ok((LogicalPlan::Filter(filter), replacements))
                }
                plan => Ok((plan, vec![])),
            }
        }
        let (input, replacements) = extend(input, &aggregates, true, state)?;
        let sorts = sorts
            .into_iter()
            .map(|sort| {
                let expr = sort
                    .expr
                    .transform_up(|expr| {
                        Ok(
                            match replacements.iter().find(|(original, _)| original == &expr) {
                                Some((_, column)) => Transformed::yes(column.clone()),
                                None => Transformed::no(expr),
                            },
                        )
                    })?
                    .data;
                Ok(Sort { expr, ..sort })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((input, sorts))
    }

    /// Spark adds the attributes missing from sort orders to every operator between the
    /// sort and the descendant that outputs them. DataFusion's logical plan builder only
    /// extends one projection, so add them here when every operator in between can carry them.
    fn add_sort_missing_inputs(
        input: LogicalPlan,
        sorts: &[Sort],
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(columns) = Self::missing_sort_columns(&input, sorts) else {
            return Ok(input);
        };
        // TODO: The physical optimizer pushes sorts below `MonotonicIdExec` and into limits
        //   (as a top-k sort), which changes the result, so do not recover more sort columns
        //   above them until it doesn't.
        // Only the operators that recovery crosses matter. A limit in another join
        // input cannot receive the sort.
        let mut plan = Some(&input);
        while let Some(node) = plan {
            let blocks = match node {
                LogicalPlan::Limit(_) => true,
                LogicalPlan::Extension(extension) => {
                    extension.node.as_any().is::<MonotonicIdNode>()
                }
                _ => false,
            };
            if blocks {
                return Ok(input);
            }
            plan = Self::missing_input_child(node, state);
        }
        Ok(Self::add_missing_inputs(&input, &columns, state)?.unwrap_or(input))
    }

    /// Returns the columns that the sort orders reference if any is missing from the input.
    fn missing_sort_columns<'a>(
        input: &LogicalPlan,
        sorts: &'a [Sort],
    ) -> Option<HashSet<&'a Column>> {
        let columns = sorts
            .iter()
            .flat_map(|sort| sort.expr.column_refs())
            .collect::<HashSet<_>>();
        (!columns
            .iter()
            .all(|column| input.schema().has_column(column)))
        .then_some(columns)
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
        let duplicate_sets = has_grouping_set
            && Self::correlation_keeps_grouping(&aggregate.input, &aggregate.group_expr)?
            && Self::has_duplicate_grouping_sets(&aggregate.group_expr)?;
        let expr =
            Self::rewrite_grouping_expr(expr, &grouping_exprs, has_grouping_set, duplicate_sets)?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort_to_projection_input(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
        Ok(Sort {
            expr: Self::rebase_to_projection_input(sort.expr, projection)?,
            ..sort
        })
    }

    fn rebase_to_projection_input(expr: Expr, projection: &[Expr]) -> PlanResult<Expr> {
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
        expr.transform_down(|e| {
            if let Expr::Column(ref col) = e
                && let Some(expr) = find(col)
            {
                return Ok(Transformed::yes(expr));
            }
            Ok(Transformed::no(e))
        })
        .data()
        .map_err(Into::into)
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
