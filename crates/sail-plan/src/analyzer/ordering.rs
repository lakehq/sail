//! Preserve explicit input ordering when an aggregate observes row order.
use sail_common_plan::deferred::Deferred;
use sail_common_plan::model::*;
use sail_common_plan::rewrite::{QueryRewriter, walk_query};

use crate::error::{PlanError, PlanResult};

pub(crate) fn preserve(plan: Query) -> PlanResult<Query> {
    Preserve.rewrite_query(plan)
}

struct Preserve;
impl QueryRewriter for Preserve {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: Query) -> PlanResult<Query> {
        let mut plan = walk_query(self, plan)?;
        if let Deferred::Resolved(QueryKind::Aggregate {
            input,
            grouping,
            aggregates,
            ..
        }) = &mut plan.kind
        {
            let mut requires_order = false;
            for projection in aggregates {
                requires_order |= projection.expression.any(&|node| {
                    let ExprKind::AggregateFunction { function, arguments, .. } = node else { return false; };
                    let name = function.name();
                    if matches!(name, "min" | "max" | "count" | "bit_and" | "bit_or" | "bit_xor" | "bool_and" | "bool_or") {
                        return false;
                    }
                    if grouping.is_empty() && matches!(name, "sum" | "avg") { return false; }
                    if matches!(name, "sum" | "avg" | "try_sum" | "try_avg") {
                        return arguments.first().is_none_or(|arg| {
                            !matches!(&arg.result_type, Deferred::Resolved(t) if !matches!(t.data_type, arrow_schema::DataType::Float16 | arrow_schema::DataType::Float32 | arrow_schema::DataType::Float64))
                        });
                    }
                    true
                })?;
            }
            if requires_order {
                require_sort(input);
            }
        }
        if let Deferred::Resolved(QueryKind::Window { input, expressions }) = &mut plan.kind
            && expressions.iter().any(|projection| matches!(&projection.expression.kind, Deferred::Resolved(ExprKind::WindowFunction { order_by, .. }) if order_by.is_empty())) {
                require_sort(input);
        }
        Ok(plan)
    }
}

fn require_sort(plan: &mut Query) {
    match &mut plan.kind {
        Deferred::Resolved(QueryKind::Sort { required, .. }) => *required = true,
        Deferred::Resolved(
            QueryKind::Scope { input }
            | QueryKind::Project { input, .. }
            | QueryKind::Filter { input, .. }
            | QueryKind::Window { input, .. }
            | QueryKind::Limit { input, .. }
            | QueryKind::RowIdentity { input, .. }
            | QueryKind::Unnest { input, .. },
        ) => require_sort(input),
        _ => {}
    }
}
