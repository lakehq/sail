use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::{ident, Expr, LogicalPlan};

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};

pub(crate) struct WindowRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    /// Cache of already-processed window functions keyed by schema name.
    /// This deduplicates identical window function expressions that appear
    /// multiple times in the expression tree (e.g. when a division-by-zero
    /// guard wraps a window-function divisor in a CASE expression).
    seen: HashMap<String, Expr>,
}

impl<'s> PlanRewriter<'s> for WindowRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self {
            plan,
            state,
            seen: HashMap::new(),
        }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for WindowRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        match node {
            Expr::WindowFunction(_) => {
                let name = node.schema_name().to_string();
                if let Some(replacement) = self.seen.get(&name) {
                    return Ok(Transformed::yes(replacement.clone()));
                }
                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                self.plan = LogicalPlan::Window(Window::try_new(vec![node], Arc::new(plan))?);
                let alias = self.state.register_field_name(&name);
                let replacement = ident(name.clone()).alias(alias);
                self.seen.insert(name, replacement.clone());
                Ok(Transformed::yes(replacement))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
