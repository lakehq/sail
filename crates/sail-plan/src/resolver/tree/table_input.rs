use std::mem;

use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{plan_datafusion_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};

use crate::extension::function::struct_function::StructFunction;
use crate::extension::function::table_input::TableInput;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};
use crate::resolver::PlanResolver;

/// Rewrites table input expression as cross join in UDTF lateral view.
pub(crate) struct TableInputRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for TableInputRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for TableInputRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = &node else {
            return Ok(Transformed::no(node));
        };
        let Some(table) = func.inner().as_any().downcast_ref::<TableInput>() else {
            return Ok(Transformed::no(node));
        };
        let plan = mem::replace(&mut self.plan, empty_logical_plan());
        let field_names = PlanResolver::get_field_names(table.plan().schema(), self.state)
            .map_err(|e| plan_datafusion_err!("{e}"))?;
        let columns = table
            .plan()
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        // The table input is replaced with a struct expression in the projection of
        // the cross join plan.
        let expr = ScalarUDF::from(StructFunction::new(field_names)).call(columns);
        self.plan = LogicalPlanBuilder::new(plan)
            .cross_join(table.plan().as_ref().clone())?
            .build()?;
        Ok(Transformed::yes(expr))
    }
}
