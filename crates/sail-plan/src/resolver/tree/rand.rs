use std::mem;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ident, Expr, Extension, LogicalPlan};
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_logical_plan::rand::{RandMode, RandNode};

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{empty_logical_plan, PlanRewriter};

pub(crate) struct RandRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for RandRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for RandRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };

        let inner = func.inner();
        let mode = if inner.as_any().downcast_ref::<Random>().is_some() {
            RandMode::Uniform
        } else if inner.as_any().downcast_ref::<Randn>().is_some() {
            RandMode::Gaussian
        } else {
            return Ok(Transformed::no(func.call(args)));
        };

        // Extract the seed from args. Only rewrite seeded calls.
        // The seed may arrive as any integer type (e.g. Int32 from PySpark proto)
        // before type coercion runs, so accept all integer scalar types.
        let seed: i64 = match args.as_slice() {
            [] => return Ok(Transformed::no(func.call(args))),
            [Expr::Literal(scalar, _)] => match scalar {
                ScalarValue::Int8(Some(v)) => *v as i64,
                ScalarValue::Int16(Some(v)) => *v as i64,
                ScalarValue::Int32(Some(v)) => *v as i64,
                ScalarValue::Int64(Some(v)) => *v,
                _ => return Ok(Transformed::no(func.call(args))),
            },
            _ => return Ok(Transformed::no(func.call(args))),
        };

        let col = self.state.register_field_name("");
        let plan = mem::replace(&mut self.plan, empty_logical_plan());
        self.plan = LogicalPlan::Extension(Extension {
            node: Arc::new(RandNode::try_new(Arc::new(plan), col.clone(), seed, mode)?),
        });

        Ok(Transformed::yes(ident(&col)))
    }
}
