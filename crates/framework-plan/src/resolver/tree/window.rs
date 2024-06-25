use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::{ident, Expr, LogicalPlan};

use crate::resolver::tree::{empty_logical_plan, ProjectionRewriter};

pub(crate) struct WindowRewriter {
    plan: LogicalPlan,
}

impl ProjectionRewriter for WindowRewriter {
    fn new_from_input(input: LogicalPlan) -> Self {
        Self { plan: input }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for WindowRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        match node {
            Expr::WindowFunction(_) => {
                let name = node.display_name()?;
                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                self.plan = LogicalPlan::Window(Window::try_new(vec![node], Arc::new(plan))?);
                Ok(Transformed::yes(ident(name)))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
