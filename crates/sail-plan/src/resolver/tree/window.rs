use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::functions_window::nth_value::{NthValue, NthValueKind};
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError};
use datafusion_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, SortExpr, WindowFrame, WindowFrameUnits, ident,
};
use sail_function::window::SparkFirstLastValue;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::RequiredSortNode;

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
    input_order_initialized: bool,
    stable_ordering: Option<Vec<SortExpr>>,
    stable_ordering_enforced: bool,
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
            input_order_initialized: false,
            stable_ordering: None,
            stable_ordering_enforced: false,
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
            Expr::WindowFunction(function) => {
                let requires_input_order =
                    Self::requires_input_order(&function, self.plan.schema())
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;

                if !self.input_order_initialized {
                    self.input_order_initialized = true;
                    let (plan, found, global) =
                        PlanResolver::require_input_sort_inner(self.plan.clone())
                            .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    self.plan = plan;
                    if found && global {
                        let mut ordering =
                            PlanResolver::input_sort_ordering(&self.plan).unwrap_or_default();
                        let column = self.state.register_hidden_field_name("");
                        self.plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(MonotonicIdNode::try_new(
                                Arc::new(self.plan.clone()),
                                column.clone(),
                            )?),
                        });
                        ordering.push(SortExpr {
                            expr: ident(column),
                            asc: true,
                            nulls_first: true,
                        });
                        self.stable_ordering = Some(ordering);
                        self.stable_ordering_enforced = true;
                    }
                }

                let mut required = function
                    .params
                    .partition_by
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: true,
                    })
                    .collect::<Vec<_>>();
                required.extend(function.params.order_by.clone());

                if let Some(prior) = self.stable_ordering.clone() {
                    let prior_satisfies_required =
                        required.is_empty() || prior.starts_with(&required);
                    let already_enforced =
                        self.stable_ordering_enforced && prior_satisfies_required;
                    let desired = if prior_satisfies_required {
                        prior
                    } else {
                        for sort in prior {
                            if !required.iter().any(|x| x.expr == sort.expr) {
                                required.push(sort);
                            }
                        }
                        required
                    };

                    if requires_input_order && !already_enforced {
                        self.plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(RequiredSortNode::new(
                                Arc::new(self.plan.clone()),
                                desired.clone(),
                                None,
                                false,
                            )),
                        });
                        self.stable_ordering_enforced = true;
                    } else if !already_enforced {
                        self.stable_ordering_enforced = false;
                    }
                    self.stable_ordering = Some(desired);
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
