//! Preserve explicit input ordering when an aggregate observes row order.
use sail_query::deferred::Deferred;
use sail_query::ir::*;
use sail_query::rewrite::{QueryRewriter, walk_plan};

use crate::error::{PlanError, PlanResult};

pub(super) fn preserve(plan: LogicalPlan) -> PlanResult<LogicalPlan> {
    Preserve.rewrite_plan(plan)
}

struct Preserve;
impl QueryRewriter for Preserve {
    type Error = PlanError;

    fn rewrite_plan(&mut self, plan: LogicalPlan) -> PlanResult<LogicalPlan> {
        let mut plan = walk_plan(self, plan)?;
        if let Deferred::Resolved(PlanNode::Aggregate {
            input,
            grouping,
            aggregates,
            ..
        }) = &mut plan.node
        {
            let mut requires_order = false;
            for projection in aggregates {
                requires_order |= projection.expression.any(&|node| {
                    let ExpressionNode::AggregateFunction { function, arguments, .. } = node else { return false; };
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
        if let Deferred::Resolved(PlanNode::Window { input, expressions }) = &mut plan.node
            && expressions.iter().any(|projection| matches!(&projection.expression.node, Deferred::Resolved(ExpressionNode::WindowFunction { order_by, .. }) if order_by.is_empty())) {
                require_sort(input);
        }
        Ok(plan)
    }
}

fn require_sort(plan: &mut LogicalPlan) {
    match &mut plan.node {
        Deferred::Resolved(PlanNode::Sort { required, .. }) => *required = true,
        Deferred::Resolved(
            PlanNode::Scope { input }
            | PlanNode::Project { input, .. }
            | PlanNode::Filter { input, .. }
            | PlanNode::Window { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::RowIdentity { input, .. }
            | PlanNode::Unnest { input, .. },
        ) => require_sort(input),
        _ => {}
    }
}
