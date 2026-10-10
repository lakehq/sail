use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, Extension, LogicalPlan, ident};
use sail_function::scalar::misc::distributed_sequence_id::SparkDistributedSequenceId;
use sail_logical_plan::distributed_sequence_id::DistributedSequenceIdNode;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{PlanRewriter, empty_logical_plan};

pub(crate) struct DistributedSequenceIdRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    column_name: Option<String>,
}

impl<'s> PlanRewriter<'s> for DistributedSequenceIdRewriter<'s> {
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

impl TreeNodeRewriter for DistributedSequenceIdRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };

        let inner = func.inner();
        if !inner.is::<SparkDistributedSequenceId>() {
            return Ok(Transformed::no(func.call(args)));
        }

        // Spark 4.2 optionally passes a cache hint. Input is always materialized here.
        match args.as_slice() {
            [] | [Expr::Literal(ScalarValue::Boolean(Some(_)), _)] => {}
            _ => {
                return plan_err!(
                    "distributed_sequence_id expects no arguments or a boolean literal"
                );
            }
        }

        let col = match &self.column_name {
            Some(c) => c.clone(),
            None => {
                // Create an internal-only field ID without a referenceable name.
                let col = self.state.next_field_id();

                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                let (plan, _, _) = PlanResolver::require_input_sort_inner(plan)
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                self.plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(DistributedSequenceIdNode::try_new(
                        Arc::new(plan),
                        col.clone(),
                    )?),
                });
                self.column_name = Some(col.clone());
                col
            }
        };

        Ok(Transformed::yes(ident(&col).alias(&col)))
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn rewrite_distributed_sequence_expressions(
        &self,
        input: LogicalPlan,
        expressions: Vec<Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<Expr>)> {
        let mut rewriter = DistributedSequenceIdRewriter::new_from_plan(input, state);
        let expressions = expressions
            .into_iter()
            .map(|expr| Ok(expr.rewrite(&mut rewriter)?.data))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expressions))
    }
}
