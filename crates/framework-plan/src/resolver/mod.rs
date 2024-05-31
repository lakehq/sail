use datafusion::prelude::SessionContext;

mod expression;
pub mod plan;
mod utils;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        Self { ctx }
    }
}
