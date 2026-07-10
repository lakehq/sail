use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::functions_aggregate::array_agg::ArrayAgg;
use datafusion::functions_aggregate::first_last::{FirstValue, LastValue};
use datafusion::functions_window::nth_value::NthValue;
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::expr::WindowFunctionDefinition;
use datafusion_expr::{Expr, LogicalPlan, SortExpr, WindowFrame, ident};
use sail_function::window::SparkFirstLastValue;

use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

pub(crate) struct WindowRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    // The input sort, captured before `plan` gets wrapped in `Window` nodes.
    input_ordering: Option<Vec<SortExpr>>,
    /// Cache of already-processed window functions keyed by schema name.
    /// This deduplicates identical window function expressions that appear
    /// multiple times in the expression tree (e.g. when a division-by-zero
    /// guard wraps a window-function divisor in a CASE expression).
    seen: HashMap<String, Expr>,
}

impl<'s> PlanRewriter<'s> for WindowRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        let input_ordering = PlanResolver::input_sort_ordering(&plan);
        Self {
            plan,
            state,
            input_ordering,
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
            Expr::WindowFunction(mut function) => {
                // Order-sensitive window functions without ORDER BY inherit the input sort.
                let order_sensitive = match &function.fun {
                    WindowFunctionDefinition::AggregateUDF(udaf) => {
                        udaf.inner().is::<FirstValue>()
                            || udaf.inner().is::<LastValue>()
                            || udaf.inner().is::<ArrayAgg>()
                    }
                    WindowFunctionDefinition::WindowUDF(udwf) => {
                        udwf.inner().is::<NthValue>() || udwf.inner().is::<SparkFirstLastValue>()
                    }
                };
                if order_sensitive
                    && !function.params.distinct
                    && function.params.order_by.is_empty()
                    && function.params.window_frame == WindowFrame::new(None)
                    && let Some(ordering) = self.input_ordering.clone()
                {
                    function.params.order_by = ordering;
                }
                let node = Expr::WindowFunction(function);
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
