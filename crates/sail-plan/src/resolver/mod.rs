use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod command;
pub(crate) mod conditional;
mod constraint;
mod data_type;
mod expression;
mod function;
mod lakehouse;
mod literal;
pub mod plan;
mod query;
mod schema;
mod state;
pub(crate) use state::ConditionalTypeContext;
mod tree;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self { ctx, config }
    }
}
