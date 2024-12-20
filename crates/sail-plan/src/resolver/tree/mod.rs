use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::{EmptyRelation, LogicalPlan};

use crate::resolver::state::PlanResolverState;

pub(crate) mod explode;
pub(crate) mod table_input;
pub(crate) mod window;

fn empty_logical_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

pub(crate) trait PlanRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self;
    fn into_plan(self) -> LogicalPlan;
}
