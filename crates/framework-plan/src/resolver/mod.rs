use crate::config::PlanConfig;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

mod data_type;
mod expression;
mod literal;
pub mod plan;
mod utils;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self { ctx, config }
    }
}
