use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result, plan_datafusion_err};
use datafusion_expr::expr::{Sort, WindowFunction, WindowFunctionParams};
use datafusion_expr::logical_plan::{FetchType, SkipType};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF, WindowFrame,
    WindowFunctionDefinition, expr_fn, ident, lit,
};
use sail_function::scalar::conditional::coalesce::CoalesceFunc;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

use crate::scalar_subquery_domain::correlated_equality_key;

/// A correlated EXISTS subquery only tests whether its body returns a row for each outer
/// row, so a LIMIT without an OFFSET at the top of the body, and the sorts below it, cannot
/// change its result. The limit is removed, or the body is replaced by an empty relation for
/// `LIMIT 0`, as the decorrelation of EXISTS does, so that the sorts do not prevent it.
/// With an OFFSET, EXISTS tests whether the body returns more rows than the offset, so
/// it is rewritten into a correlated count, as projected EXISTS is, unless `DecorrelateLimit`
/// handles its correlation.
#[derive(Debug)]
pub struct RemoveExistsLimit;

impl OptimizerRule for RemoveExistsLimit {
    fn name(&self) -> &str {
        "remove_exists_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut found = false;
        plan.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::Exists(exists) = expr
                    && !exists.subquery.outer_ref_columns.is_empty()
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        if !found {
            return Ok(Transformed::no(plan));
        }
        plan.map_expressions(|expr| {
            expr.transform_up(|expr| {
                let Expr::Exists(mut exists) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if exists.subquery.outer_ref_columns.is_empty() {
                    return Ok(Transformed::no(Expr::Exists(exists)));
                }
                if let Some((input, skip)) = top_level_offset(&exists.subquery.subquery)?
                    && !has_only_equality_correlation(&input)?
                {
                    let result = count_rows(input, skip, config)?;
                    return Ok(Transformed::yes(if exists.negated {
                        !result
                    } else {
                        result
                    }));
                }
                let body = remove_top_level_limit(exists.subquery.subquery.as_ref().clone())?;
                if !body.transformed {
                    return Ok(Transformed::no(Expr::Exists(exists)));
                }
                exists.subquery.outer_ref_columns = body.data.all_out_ref_exprs();
                exists.subquery.subquery = Arc::new(body.data);
                Ok(Transformed::yes(Expr::Exists(exists)))
            })
        })
    }
}

/// Returns the input and the offset of the limit with a nonzero offset and a nonzero
/// fetch at the top of the plan.
fn top_level_offset(plan: &LogicalPlan) -> Result<Option<(LogicalPlan, usize)>> {
    match plan {
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            top_level_offset(plan.inputs()[0])
        }
        LogicalPlan::Limit(limit) => match (limit.get_skip_type()?, limit.get_fetch_type()?) {
            (SkipType::Literal(skip), FetchType::Literal(fetch))
                if skip > 0 && fetch != Some(0) =>
            {
                Ok(Some((limit.input.as_ref().clone(), skip)))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Tests whether the plan returns more than `skip` rows.
fn count_rows(plan: LogicalPlan, skip: usize, config: &dyn OptimizerConfig) -> Result<Expr> {
    let plan = remove_top_level_sorts(plan)?.data;
    if is_scalar_aggregate(&plan) {
        // A scalar aggregation returns exactly one row, and the offset is at least one.
        return Ok(lit(false));
    }
    let output = config.alias_generator().next("__sail_exists_count");
    exists_by_count(plan, skip, &output, |_| Ok(()))
}

/// Tests whether the query returns more than `skip` rows, as
/// `coalesce((SELECT count(1) > skip FROM query), false)`. `validate` checks the counting query
/// before it becomes a scalar subquery.
pub fn exists_by_count(
    query: LogicalPlan,
    skip: usize,
    output: &str,
    validate: impl FnOnce(&LogicalPlan) -> Result<()>,
) -> Result<Expr> {
    let skip = i64::try_from(skip).map_err(|error| plan_datafusion_err!("{error}"))?;
    // COUNT produces one non-null result even when the subquery has no rows.
    let query = LogicalPlanBuilder::from(query)
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64)).alias(output)])?
        .project(vec![ident(output).gt(lit(skip))])?
        .build()?;
    validate(&query)?;
    Ok(ScalarUDF::from(CoalesceFunc::new())
        .call(vec![expr_fn::scalar_subquery(Arc::new(query)), lit(false)]))
}

fn is_scalar_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            is_scalar_aggregate(plan.inputs()[0])
        }
        LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
        _ => false,
    }
}

/// Removes the limit without an offset at the top of the plan, and the sorts below it.
fn remove_top_level_limit(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            plan.map_children(remove_top_level_limit)
        }
        LogicalPlan::Limit(limit) => {
            let (SkipType::Literal(0), FetchType::Literal(fetch)) =
                (limit.get_skip_type()?, limit.get_fetch_type()?)
            else {
                return Ok(Transformed::no(LogicalPlan::Limit(limit)));
            };
            if fetch == Some(0) {
                return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                    EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::clone(limit.input.schema()),
                    },
                )));
            }
            Ok(Transformed::yes(
                remove_top_level_sorts(Arc::unwrap_or_clone(limit.input))?.data,
            ))
        }
        plan => Ok(Transformed::no(plan)),
    }
}

/// Removes the sorts at the top of the plan, which select no rows.
fn remove_top_level_sorts(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            plan.map_children(remove_top_level_sorts)
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => Ok(Transformed::yes(
            remove_top_level_sorts(Arc::unwrap_or_clone(sort.input))?.data,
        )),
        LogicalPlan::Extension(ref extension)
            if extension
                .node
                .as_any()
                .downcast_ref::<SortWithinPartitionsNode>()
                .is_some_and(|sort| sort.fetch().is_none()) =>
        {
            let input = extension.node.inputs()[0].clone();
            Ok(Transformed::yes(remove_top_level_sorts(input)?.data))
        }
        plan => Ok(Transformed::no(plan)),
    }
}

/// A correlated LIMIT applies independently to every equality key. Rank the
/// decorrelated input within each key, then put the correlation above the rank.
#[derive(Debug)]
pub struct DecorrelateLimit;

impl OptimizerRule for DecorrelateLimit {
    fn name(&self) -> &str {
        "decorrelate_limit"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let outer = limit.input.all_out_ref_exprs();
        if outer.is_empty() {
            return Ok(Transformed::no(plan));
        }
        let (SkipType::Literal(skip), FetchType::Literal(fetch)) =
            (limit.get_skip_type()?, limit.get_fetch_type()?)
        else {
            return Ok(Transformed::no(plan));
        };
        let mut keys = BTreeSet::new();
        let mut eligible = true;
        limit.input.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                for predicate in split_conjunction(&filter.predicate) {
                    if !predicate.contains_outer() {
                        continue;
                    }
                    if let Some(key) = correlated_equality_key(predicate, false) {
                        keys.insert(key.clone());
                    } else {
                        eligible = false;
                    }
                }
            }
            if matches!(
                node,
                LogicalPlan::Aggregate(_)
                    | LogicalPlan::Window(_)
                    | LogicalPlan::Limit(_)
                    | LogicalPlan::Subquery(_)
            ) {
                eligible = false;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !eligible || keys.is_empty() {
            return Ok(Transformed::no(plan));
        }
        let output = plan.schema().columns();
        let (input, order) =
            extract_ordering(Arc::unwrap_or_clone(Arc::clone(&limit.input)), skip, fetch)?;
        let mut pull_up = PullUpCorrelatedExpr::new();
        let input = input.rewrite(&mut pull_up)?.data;
        if !pull_up.can_pull_up {
            return Ok(Transformed::no(plan));
        }
        let rank_name = config.alias_generator().next("__sail_limit_rank");
        let rank = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            params: WindowFunctionParams {
                args: vec![],
                partition_by: keys.into_iter().map(Expr::Column).collect(),
                order_by: order.unwrap_or_default(),
                window_frame: WindowFrame::new(None),
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        }))
        .alias(&rank_name);
        let rank_column = Expr::Column(Column::new_unqualified(rank_name));
        let mut predicate = rank_column.clone().gt(lit(skip as u64));
        if let Some(fetch) = fetch {
            predicate = predicate.and(rank_column.lt_eq(lit(skip.saturating_add(fetch) as u64)));
        }
        let Some(correlation) = conjunction(pull_up.join_filters) else {
            return Ok(Transformed::no(plan));
        };
        let correlation = correlation.transform_up(|expr| {
            if let Expr::Column(column) = &expr
                && let Some(reference) = outer.iter().find(|reference| matches!(reference, Expr::OuterReferenceColumn(_, c) if c == column)) {
                    return Ok(Transformed::yes(reference.clone()));
            }
            Ok(Transformed::no(expr))
        })?.data;
        let result = LogicalPlanBuilder::from(input)
            .window(vec![rank])?
            .filter(predicate)?
            .filter(correlation)?
            .project(output.into_iter().map(Expr::Column))?
            .build()?;
        Ok(Transformed::yes(result))
    }
}

/// Whether the correlated predicates of the plan are all equalities with a plain column, which
/// `DecorrelateLimit` handles.
fn has_only_equality_correlation(plan: &LogicalPlan) -> Result<bool> {
    let (mut equality, mut other) = (false, false);
    plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            for predicate in split_conjunction(&filter.predicate) {
                if predicate.contains_outer() {
                    if correlated_equality_key(predicate, false).is_some() {
                        equality = true;
                    } else {
                        other = true;
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(equality && !other)
}

/// Carry ordering expressions through projections before ranking. The outer
/// projection restores the LIMIT's original output after the extra keys are used.
fn extract_ordering(
    plan: LogicalPlan,
    skip: usize,
    fetch: Option<usize>,
) -> Result<(LogicalPlan, Option<Vec<Sort>>)> {
    match plan {
        LogicalPlan::Sort(sort)
            if sort
                .fetch
                .is_none_or(|top| fetch.is_some_and(|fetch| top >= skip.saturating_add(fetch))) =>
        {
            Ok((Arc::unwrap_or_clone(sort.input), Some(sort.expr)))
        }
        LogicalPlan::Extension(ref extension)
            if let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>()
                && sort.fetch().is_none_or(|top| {
                    fetch.is_some_and(|fetch| top >= skip.saturating_add(fetch))
                })
                && !sort.preserve_partitioning() =>
        {
            Ok((
                extension.node.inputs()[0].clone(),
                Some(sort.sort_expr().to_vec()),
            ))
        }
        LogicalPlan::Projection(projection) => {
            let (input, ordering) =
                extract_ordering(Arc::unwrap_or_clone(projection.input), skip, fetch)?;
            let Some(ordering) = ordering else {
                return Ok((
                    LogicalPlan::Projection(Projection::try_new(projection.expr, Arc::new(input))?),
                    None,
                ));
            };
            let outputs = projection.schema.columns();
            let mut expressions = projection.expr.clone();
            let ordering = ordering
                .into_iter()
                .map(|sort| {
                    let expr = sort
                        .expr
                        .transform_down(|expr| {
                            if let Some(index) = projection
                                .expr
                                .iter()
                                .position(|projected| projected.clone().unalias() == expr)
                            {
                                return Ok(Transformed::new(
                                    Expr::Column(outputs[index].clone()),
                                    true,
                                    TreeNodeRecursion::Jump,
                                ));
                            }
                            if let Expr::Column(column) = &expr
                                && !outputs.contains(column)
                                && !expressions.contains(&expr)
                            {
                                expressions.push(expr.clone());
                            }
                            Ok(Transformed::no(expr))
                        })?
                        .data;
                    Ok(Sort { expr, ..sort })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((
                LogicalPlan::Projection(Projection::try_new(expressions, Arc::new(input))?),
                Some(ordering),
            ))
        }
        LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
            let (input, ordering) = extract_ordering(plan.inputs()[0].clone(), skip, fetch)?;
            let ordering = if let LogicalPlan::SubqueryAlias(alias) = &plan {
                ordering
                    .map(|ordering| {
                        ordering
                            .into_iter()
                            .map(|sort| {
                                let expr = sort
                                    .expr
                                    .transform_up(|expr| {
                                        if let Expr::Column(mut column) = expr {
                                            column.relation = Some(alias.alias.clone());
                                            Ok(Transformed::yes(Expr::Column(column)))
                                        } else {
                                            Ok(Transformed::no(expr))
                                        }
                                    })?
                                    .data;
                                Ok(Sort { expr, ..sort })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?
            } else {
                ordering
            };
            Ok((
                plan.with_new_exprs(plan.expressions(), vec![input])?,
                ordering,
            ))
        }
        plan => Ok((plan, None)),
    }
}
