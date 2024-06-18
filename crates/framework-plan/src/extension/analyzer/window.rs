use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Column;
use datafusion_expr::{Expr, LogicalPlan};

pub(crate) fn rewrite_window(
    input: LogicalPlan,
    expr: Vec<Expr>,
) -> Result<(LogicalPlan, Vec<Expr>)> {
    let mut rewriter = WindowRewriter { plan: input };
    let expr = expr
        .into_iter()
        .map(|e| Ok(e.rewrite(&mut rewriter)?.data))
        .collect::<Result<Vec<_>>>()?;
    Ok((rewriter.plan, expr))
}

struct WindowRewriter {
    plan: LogicalPlan,
}

impl TreeNodeRewriter for WindowRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        match node {
            Expr::WindowFunction(_) => {
                let name = node.display_name()?;
                self.plan =
                    LogicalPlan::Window(Window::try_new(vec![node], Arc::new(self.plan.clone()))?);
                let node = Expr::Column(Column::new_unqualified(name));
                Ok(Transformed::yes(node))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
