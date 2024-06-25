use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::DFSchema;
use datafusion_expr::{EmptyRelation, Expr, LogicalPlan};

pub(crate) mod explode;
pub(crate) mod window;

fn empty_logical_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

pub(crate) trait ProjectionRewriter: TreeNodeRewriter<Node = Expr> {
    fn new_from_input(input: LogicalPlan) -> Self;
    fn into_plan(self) -> LogicalPlan;
}
