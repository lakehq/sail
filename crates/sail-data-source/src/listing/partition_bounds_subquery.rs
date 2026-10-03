//! Inlines a partition-bound scalar subquery so that the outer scan can narrow its
//! listing.
//!
//! `WHERE dt = (SELECT max(dt) FROM t)` is how the latest partition of a table is
//! usually asked for. The inner half costs nothing once
//! [`ResolvePartitionBounds`](crate::listing::partition_bounds::ResolvePartitionBounds)
//! has turned it into a `PartitionBounds` node, but the outer scan still lists every
//! partition: its predicate holds a subquery whose value is unknown while the scan is
//! planned, so `evaluate_partition_prefix` has nothing to narrow the listing with.
//!
//! This runs after logical optimization, where reaching the object store is allowed.
//! Substituting the value early is sound because the subquery is uncorrelated and
//! reads only directory names.
//!
//! Left alone, each gaining nothing rather than being wrong: a filter separated from
//! its table by another operator, one the scan cannot apply exactly, one over a scan
//! that already carries filters, and a subquery over a node producing more than one
//! bound. All are covered by tests asserting they still answer correctly.
//!
//! Registration is unconditional, but a `PartitionBounds` node only exists while
//! `execution.partition_bounds_from_listing` is on, so with the setting off this walks
//! the plan once and returns it untouched.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::{Expr, Filter, LogicalPlan, TableProviderFilterPushDown};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue};
use sail_common_datafusion::logical_rewriter::LogicalRewriter;

use crate::listing::partition_bounds::{PartitionBoundsNode, resolve_partition_bounds};

#[derive(Debug, Default)]
pub struct InlinePartitionBoundsSubquery;

#[async_trait]
impl LogicalRewriter for InlinePartitionBoundsSubquery {
    fn name(&self) -> &str {
        "inline_partition_bounds_subquery"
    }

    async fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &dyn Session,
    ) -> Result<Transformed<LogicalPlan>> {
        // Collected first so the tree walk stays synchronous: resolving a bound has to
        // reach the object store.
        let mut nodes = vec![];
        plan.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                collect_bounds_subqueries(&filter.predicate, &mut nodes)?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if nodes.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let mut resolved = Vec::with_capacity(nodes.len());
        for node in nodes {
            let values = resolve_partition_bounds(ctx, &node).await?;
            let [value] = values.as_slice() else {
                continue;
            };
            resolved.push((node, value.clone()));
        }
        if resolved.is_empty() {
            return Ok(Transformed::no(plan));
        }

        plan.transform_up(|node| match node {
            LogicalPlan::Filter(filter) => inline_into_filter(filter, &resolved),
            other => Ok(Transformed::no(other)),
        })
        .data()
        .map(Transformed::yes)
    }
}

/// Collects the `PartitionBounds` node behind every uncorrelated scalar subquery of
/// `predicate` that produces exactly one bound.
fn collect_bounds_subqueries(predicate: &Expr, out: &mut Vec<PartitionBoundsNode>) -> Result<()> {
    predicate.apply(|expr| {
        if let Expr::ScalarSubquery(subquery) = expr
            && subquery.outer_ref_columns.is_empty()
            && let Some(node) = partition_bounds_of(subquery.subquery.as_ref())
            && node.bounds().len() == 1
        {
            out.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Looks through the renaming projections a subquery plan ends in and returns the
/// `PartitionBounds` node underneath, if that is all the plan does.
fn partition_bounds_of(plan: &LogicalPlan) -> Option<&PartitionBoundsNode> {
    match plan {
        LogicalPlan::Projection(projection) => {
            // Only a pure rename may be looked through: anything computed on top of
            // the bound would change the value the subquery produces.
            if !projection.expr.iter().all(is_rename) {
                return None;
            }
            partition_bounds_of(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => partition_bounds_of(alias.input.as_ref()),
        LogicalPlan::Extension(extension) => extension.node.as_any().downcast_ref(),
        _ => None,
    }
}

/// Whether any subquery is left in the expression after the substitutions.
fn has_subquery(expr: &Expr) -> Result<bool> {
    let mut found = false;
    expr.apply(|e| {
        if matches!(
            e,
            Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
        ) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn is_rename(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => is_rename(alias.expr.as_ref()),
        _ => false,
    }
}

/// Replaces the resolved subqueries inside a filter and, when what remains is a
/// filter the scan can apply exactly, moves it into the scan.
fn inline_into_filter(
    filter: Filter,
    resolved: &[(PartitionBoundsNode, ScalarValue)],
) -> Result<Transformed<LogicalPlan>> {
    let predicate = filter.predicate.clone().transform_up(|expr| match &expr {
        // Repeated rather than left to the collection step: a correlated subquery must
        // never be replaced by a value computed once.
        Expr::ScalarSubquery(subquery) if subquery.outer_ref_columns.is_empty() => {
            match partition_bounds_of(subquery.subquery.as_ref())
                .and_then(|node| resolved.iter().find(|(candidate, _)| candidate == node))
            {
                Some((_, value)) => Ok(Transformed::yes(Expr::Literal(value.clone(), None))),
                None => Ok(Transformed::no(expr)),
            }
        }
        _ => Ok(Transformed::no(expr)),
    })?;
    if !predicate.transformed {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }
    let predicate = predicate.data;

    // A predicate that still holds a subquery must stay above the scan: pushing one
    // down hands the physical planner an expression it cannot build.
    if has_subquery(&predicate)? {
        return Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::clone(&filter.input),
        )?)));
    }

    // The filter is only worth moving into the scan when the scan can apply it in
    // full; otherwise it has to stay where it is and the listing cannot be narrowed.
    if let LogicalPlan::TableScan(scan) = filter.input.as_ref()
        && scan.filters.is_empty()
        && matches!(
            scan.source
                .supports_filters_pushdown(&[&predicate])?
                .as_slice(),
            [TableProviderFilterPushDown::Exact]
        )
    {
        let mut scan = scan.clone();
        scan.filters.push(predicate);
        return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
    }

    Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
        predicate,
        Arc::clone(&filter.input),
    )?)))
}
