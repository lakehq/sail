use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{internal_err, not_impl_err, plan_err, Result};
use datafusion_expr::{col, or, Filter, Projection};
use sail_streaming::field::MARKER_FIELD_NAME;
use sail_streaming::logical_plan::sink::StreamSinkNode;
use sail_streaming::logical_plan::source::StreamSourceNode;

use crate::extension::logical::{FileWriteNode, RangeNode};

struct StreamingRewriter;

impl StreamingRewriter {
    fn f_up_extension(&mut self, extension: Extension) -> Result<Transformed<LogicalPlan>> {
        let node = extension.node.as_ref();
        if node.as_any().is::<RangeNode>() {
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(StreamSourceNode::try_new(Arc::new(
                    LogicalPlan::Extension(extension),
                ))?),
            })))
        } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let input = Arc::new(LogicalPlan::Extension(Extension {
                node: Arc::new(StreamSinkNode::try_new(node.input().clone())?),
            }));
            let write = FileWriteNode::new(input, node.options().clone());
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(write),
            })))
        } else {
            plan_err!("unsupported extension node for streaming: {node:?}")
        }
    }
}

impl TreeNodeRewriter for StreamingRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Extension(extension) => self.f_up_extension(extension),
            LogicalPlan::Projection(Projection {
                mut expr, input, ..
            }) => {
                expr.insert(0, col(MARKER_FIELD_NAME));
                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new(expr, input)?,
                )))
            }
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let predicate = or(predicate, col(MARKER_FIELD_NAME).is_not_null());
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                    predicate, input,
                )?)))
            }
            LogicalPlan::Window(_) => {
                not_impl_err!("streaming window: {plan:?}")
            }
            LogicalPlan::Aggregate(_) => {
                not_impl_err!("streaming aggregate: {plan:?}")
            }
            LogicalPlan::Sort(_) => {
                plan_err!("sort is not supported for streaming: {plan:?}")
            }
            LogicalPlan::Join(_) => {
                not_impl_err!("streaming join: {plan:?}")
            }
            LogicalPlan::Repartition(_) => {
                not_impl_err!("streaming repartition: {plan:?}")
            }
            LogicalPlan::TableScan(_) => {
                // We need better support for different table sources.
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamSourceNode::try_new(Arc::new(plan))?),
                })))
            }
            LogicalPlan::Union(_) | LogicalPlan::SubqueryAlias(_) => Ok(Transformed::no(plan)),
            LogicalPlan::Limit(_) => {
                // We could support limit in the future, where the execution plan
                // emits all the data within the limit and passthrough markers from the input.
                plan_err!("limit is not supported for streaming: {plan:?}")
            }
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(StreamSourceNode::try_new(Arc::new(plan))?),
                })))
            }
            LogicalPlan::Unnest(_) => {
                // We need to preserve all markers in the unnested record batches.
                // This can be done by having a placeholder one-element nested value
                // for each marker row.
                not_impl_err!("streaming unnest: {plan:?}")
            }
            LogicalPlan::RecursiveQuery(_) => {
                not_impl_err!("recursive streaming query: {plan:?}")
            }
            LogicalPlan::Subquery(_) | LogicalPlan::Distinct(_) => {
                internal_err!("not rewritten before streaming rewriter: {plan:?}")
            }
            LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => {
                internal_err!("unexpected command for streaming rewriter: {plan:?}")
            }
        }
    }
}

/// Rewrite a logical plan for streaming execution.
/// This function needs to be called on an optimized logical plan, and after
/// all logical commands are executed. An error will be returned if the plan
/// contains logical command nodes or nodes that should be eliminated by the
/// optimizer (e.g. subquery).
pub fn rewrite_streaming_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let node = plan.rewrite(&mut StreamingRewriter)?;
    Ok(node.data)
}
