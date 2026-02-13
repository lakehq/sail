use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ident, Expr, Extension, LogicalPlan};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId;
use sail_logical_plan::monotonic_id::MonotonicIdNode;

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};

pub(crate) struct MonotonicIdRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    column_name: Option<String>,
}

impl<'s> PlanRewriter<'s> for MonotonicIdRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self {
            plan,
            state,
            column_name: None,
        }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for MonotonicIdRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };

        let inner = func.inner();
        if inner
            .as_any()
            .downcast_ref::<SparkMonotonicallyIncreasingId>()
            .is_none()
        {
            return Ok(Transformed::no(func.call(args)));
        }

        // `monotonically_increasing_id()` is nullary
        args.zero()?;
        let original_expr = func.call(vec![]);

        let col = match &self.column_name {
            Some(c) => c.clone(),
            None => {
                // Create an internal-only field id (external name is empty)
                let col = self.state.register_field_name("");

                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                self.plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(MonotonicIdNode::try_new(Arc::new(plan), col.clone())?),
                });
                self.column_name = Some(col.clone());
                col
            }
        };
        let rewritten = ident(&col);
        self.state
            .register_expression_rewrite(original_expr, rewritten.clone());

        Ok(Transformed::yes(rewritten))
    }
}
