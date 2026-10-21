use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, LogicalPlan};

use crate::range::RangeNode;

/// Reads a non-negative integer literal of any integer type.
pub fn integer_literal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(n)), _) => usize::try_from(*n).ok(),
        Expr::Literal(ScalarValue::Int16(Some(n)), _) => usize::try_from(*n).ok(),
        Expr::Literal(ScalarValue::Int32(Some(n)), _) => usize::try_from(*n).ok(),
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => usize::try_from(*n).ok(),
        Expr::Literal(ScalarValue::UInt8(Some(n)), _) => Some(usize::from(*n)),
        Expr::Literal(ScalarValue::UInt16(Some(n)), _) => Some(usize::from(*n)),
        Expr::Literal(ScalarValue::UInt32(Some(n)), _) => usize::try_from(*n).ok(),
        Expr::Literal(ScalarValue::UInt64(Some(n)), _) => usize::try_from(*n).ok(),
        _ => None,
    }
}

/// Conservative bounds `(min, max)` on the number of rows a plan produces, where a `None`
/// upper bound means unbounded. Unlike `LogicalPlan::max_rows`, LIMIT literals of any
/// integer type and Sail's own `range` relation are understood.
pub fn row_bounds(plan: &LogicalPlan) -> (usize, Option<usize>) {
    match plan {
        LogicalPlan::Projection(projection) => row_bounds(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => row_bounds(&alias.input),
        LogicalPlan::Sort(sort) => {
            let (min, max) = row_bounds(&sort.input);
            match sort.fetch {
                Some(fetch) => (
                    min.min(fetch),
                    Some(max.map_or(fetch, |max| max.min(fetch))),
                ),
                None => (min, max),
            }
        }
        LogicalPlan::Limit(limit) => {
            let skip = match &limit.skip {
                None => Some(0),
                Some(skip) => integer_literal(skip),
            };
            let fetch = match &limit.fetch {
                None => Some(None),
                Some(fetch) => integer_literal(fetch).map(Some),
            };
            let (Some(skip), Some(fetch)) = (skip, fetch) else {
                return (0, None);
            };
            let (min, max) = row_bounds(&limit.input);
            let min = min.saturating_sub(skip);
            let max = max.map(|max| max.saturating_sub(skip));
            match fetch {
                Some(fetch) => (
                    min.min(fetch),
                    Some(max.map_or(fetch, |max| max.min(fetch))),
                ),
                None => (min, max),
            }
        }
        LogicalPlan::Aggregate(aggregate) if aggregate.group_expr.is_empty() => (1, Some(1)),
        LogicalPlan::Aggregate(aggregate) => (0, row_bounds(&aggregate.input).1),
        LogicalPlan::Filter(filter) => (0, row_bounds(&filter.input).1),
        LogicalPlan::Values(values) => (values.values.len(), Some(values.values.len())),
        LogicalPlan::EmptyRelation(relation) => {
            let rows = usize::from(relation.produce_one_row);
            (rows, Some(rows))
        }
        LogicalPlan::Extension(extension) => {
            match extension.node.as_any().downcast_ref::<RangeNode>() {
                Some(range) => (range.range().len(), Some(range.range().len())),
                None => (0, None),
            }
        }
        _ => (0, None),
    }
}
