use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::functions_window::nth_value::{NthValue, NthValueKind};
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion_expr::{Expr, LogicalPlan, WindowFrame, WindowFrameUnits, ident};
use sail_function::window::SparkFirstLastValue;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

pub(crate) struct WindowRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    /// Cache of already-processed window functions keyed by schema name.
    /// This deduplicates identical window function expressions that appear
    /// multiple times in the expression tree (e.g. when a division-by-zero
    /// guard wraps a window-function divisor in a CASE expression).
    seen: HashMap<String, Expr>,
}

impl WindowRewriter<'_> {
    fn requires_input_order(function: &WindowFunction, schema: &DFSchemaRef) -> PlanResult<bool> {
        let function_requires_order = match &function.fun {
            WindowFunctionDefinition::AggregateUDF(udaf) => {
                !PlanResolver::is_order_irrelevant_udaf(
                    udaf.as_ref(),
                    &function.params.args,
                    schema,
                )?
            }
            WindowFunctionDefinition::WindowUDF(udwf) => {
                udwf.inner().downcast_ref::<NthValue>().is_some_and(|nth| {
                    matches!(nth.kind(), NthValueKind::First | NthValueKind::Last)
                }) || udwf.inner().is::<SparkFirstLastValue>()
            }
        };
        let frame_requires_order =
            matches!(&function.fun, WindowFunctionDefinition::AggregateUDF(_))
                && function.params.window_frame.units == WindowFrameUnits::Rows
                && function.params.window_frame != WindowFrame::new(None);
        Ok(function_requires_order || frame_requires_order)
    }
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
            Expr::WindowFunction(mut function) => {
                if Self::requires_input_order(&function, self.plan.schema())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
                    && function.params.order_by.is_empty()
                {
                    let mut attached = false;
                    if function.params.window_frame.units == WindowFrameUnits::Rows
                        && let Some(mut ordering) = PlanResolver::input_sort_ordering(&self.plan)
                    {
                        let partition_len = function.params.partition_by.len();
                        let partition_is_prefix = ordering.len() >= partition_len
                            && function.params.partition_by.iter().zip(&ordering).all(
                                |(partition, sort)| {
                                    sort.asc && sort.nulls_first && partition == &sort.expr
                                },
                            );
                        if partition_is_prefix {
                            ordering.drain(..partition_len);
                        }
                        // The ordering may be exactly the partition keys. Nothing left to attach.
                        if !ordering.is_empty() {
                            function.params.order_by = ordering;
                            attached = true;
                        }
                    }
                    if !attached {
                        self.plan = PlanResolver::require_input_sort_inner(self.plan.clone())
                            .map_err(|error| DataFusionError::External(Box::new(error)))?
                            .0;
                    }
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
