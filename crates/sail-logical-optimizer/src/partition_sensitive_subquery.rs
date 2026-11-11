use std::mem;
use std::sync::Arc;

use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{Expr, Filter, LogicalPlan, Projection};
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::seeded_random::SeededRandomNode;
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::scalar_subquery_domain::correlated_equality_key;

/// Moves the correlated predicates of the filters below an operator that depends on the rows
/// of each partition above the operator, so that the subquery can be decorrelated.
///
/// Spark removes the correlated predicates from a subquery when it decorrelates it, so the
/// operator evaluates the rows before them. Below an aggregate or a window, only the
/// equalities of an inner key and an outer expression are removed (`canPullUpOverAgg`). When
/// the predicates cannot be moved, the expressions of a [`SeededRandomNode`] are evaluated by
/// a projection, where the calls of random functions with a seed are scalar functions that are
/// not seeded per partition.
#[derive(Debug, Default)]
pub struct PullUpCorrelatedFiltersAbovePartitionSensitiveNodes;

impl OptimizerRule for PullUpCorrelatedFiltersAbovePartitionSensitiveNodes {
    fn name(&self) -> &str {
        "pull_up_correlated_filters_above_partition_sensitive_nodes"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !contains_blocked(&plan)? {
            return Ok(Transformed::no(plan));
        }
        rewrite_subqueries(plan)
    }
}

fn is_partition_sensitive(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Extension(extension) = plan else {
        return false;
    };
    let node = extension.node.as_any();
    node.is::<SeededRandomNode>()
        || node.is::<MonotonicIdNode>()
        || node.is::<SparkPartitionIdNode>()
}

fn is_correlated(plan: &LogicalPlan) -> bool {
    plan.exists(|plan| Ok(plan.contains_outer_reference()))
        .unwrap_or(true)
}

/// Returns whether the plan is an operator that depends on the rows of each partition over
/// an input with outer references, which prevents the decorrelation of the subquery.
fn is_blocked(plan: &LogicalPlan) -> bool {
    is_partition_sensitive(plan) && plan.inputs().into_iter().any(is_correlated)
}

fn rewrite_subqueries(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    plan.transform_up(|node| {
        node.map_expressions(|expr| {
            expr.transform_up(|expr| match expr {
                Expr::ScalarSubquery(mut query) => {
                    let subquery = Arc::unwrap_or_clone(mem::take(&mut query.subquery));
                    Ok(rewrite_subquery(subquery)?.update_data(|subquery| {
                        query.subquery = Arc::new(subquery);
                        Expr::ScalarSubquery(query)
                    }))
                }
                Expr::Exists(mut exists) => {
                    let subquery = Arc::unwrap_or_clone(mem::take(&mut exists.subquery.subquery));
                    Ok(rewrite_subquery(subquery)?.update_data(|subquery| {
                        exists.subquery.subquery = Arc::new(subquery);
                        Expr::Exists(exists)
                    }))
                }
                Expr::InSubquery(mut query) => {
                    let subquery = Arc::unwrap_or_clone(mem::take(&mut query.subquery.subquery));
                    Ok(rewrite_subquery(subquery)?.update_data(|subquery| {
                        query.subquery.subquery = Arc::new(subquery);
                        Expr::InSubquery(query)
                    }))
                }
                expr => Ok(Transformed::no(expr)),
            })
        })
    })
}

/// Rewrites the plan of an `EXISTS` or `IN` subquery as the rule does, for a subquery that is
/// decorrelated before the optimizer runs.
pub fn pull_up_correlated_filters_in_predicate_subquery(
    plan: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    if !contains_blocked(&plan)? {
        return Ok(Transformed::no(plan));
    }
    rewrite_subquery(plan)
}

fn contains_blocked(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = is_blocked(node);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn rewrite_subquery(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    rewrite_subqueries(plan)?.transform_data(|plan| {
        if plan.exists(|node| Ok(is_blocked(node)))? {
            unblock(plan, false)
        } else {
            Ok(Transformed::no(plan))
        }
    })
}

/// Rewrites the blocked operators in the plan, from the bottom up, so that an operator over
/// another one sees its rewritten input. `aggregated` is whether an aggregate or a window is
/// above the plan in the subquery.
fn unblock(plan: LogicalPlan, aggregated: bool) -> Result<Transformed<LogicalPlan>> {
    let aggregated =
        aggregated || matches!(plan, LogicalPlan::Aggregate(_) | LogicalPlan::Window(_));
    plan.map_children(|child| unblock(child, aggregated))?
        .transform_data(|plan| {
            if !is_blocked(&plan) {
                return Ok(Transformed::no(plan));
            }
            if let Some(plan) = pull_up(&plan, aggregated)? {
                return Ok(Transformed::yes(plan));
            }
            dissolve(plan)
        })
}

/// Moves the correlated predicates of the filters directly below the operator above it, if the
/// input of the filters is uncorrelated and every correlated predicate can be moved.
fn pull_up(plan: &LogicalPlan, aggregated: bool) -> Result<Option<LogicalPlan>> {
    let Some(mut input) = plan.inputs().into_iter().next().cloned() else {
        return Ok(None);
    };
    let mut predicates = vec![];
    while let LogicalPlan::Filter(filter) = input {
        predicates.push(filter.predicate);
        input = Arc::unwrap_or_clone(filter.input);
    }
    if is_correlated(&input) {
        return Ok(None);
    }
    let mut correlated = vec![];
    let mut remaining = vec![];
    for predicate in predicates {
        let (pulled, kept): (Vec<_>, Vec<_>) = split_conjunction_owned(predicate)
            .into_iter()
            .partition(Expr::contains_outer);
        if aggregated
            && !pulled
                .iter()
                .all(|e| correlated_equality_key(e, true).is_some())
        {
            return Ok(None);
        }
        correlated.extend(pulled);
        remaining.push(conjunction(kept));
    }
    let Some(correlated) = conjunction(correlated) else {
        return Ok(None);
    };
    for predicate in remaining.into_iter().rev().flatten() {
        input = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?);
    }
    let plan = plan.with_new_exprs(plan.expressions(), vec![input])?;
    Ok(Some(LogicalPlan::Filter(Filter::try_new(
        correlated,
        Arc::new(plan),
    )?)))
}

/// Evaluates the expressions of a [`SeededRandomNode`] by a projection. Other operators are
/// kept.
fn dissolve(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Extension(extension) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let Some(node) = extension.node.as_any().downcast_ref::<SeededRandomNode>() else {
        return Ok(Transformed::no(plan));
    };
    let input = node.input();
    let exprs = input
        .schema()
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
        .chain(
            node.columns()
                .iter()
                .map(|column| column.expr.clone().unalias().alias(&column.name)),
        )
        .collect();
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new(exprs, Arc::clone(input))?,
    )))
}
