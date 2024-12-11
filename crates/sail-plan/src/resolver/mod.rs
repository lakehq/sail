use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod data_source;
mod data_type;
mod ddl;
mod expression;
mod function;
mod literal;
pub mod plan;
mod schema;
mod state;
mod statistic;
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
