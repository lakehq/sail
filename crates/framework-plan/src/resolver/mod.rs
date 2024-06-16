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

pub struct PlanResolverState {
    next_id: usize,
}

impl PlanResolverState {
    pub fn new() -> Self {
        Self { next_id: 0 }
    }

    pub fn next_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}
