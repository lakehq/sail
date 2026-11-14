use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::shared_cte::SharedCteConfig;

use crate::config::PlanConfig;

mod command;
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
mod tree;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    /// Whether the repeated references to a CTE are resolved as scans of the CTE.
    share_ctes: bool,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            share_ctes: false,
        }
    }

    /// Resolves the repeated references to a CTE as scans of the CTE, if the session
    /// shares CTEs. The resolved plan must then be optimized and planned with
    /// [`optimize_and_plan_shared_ctes`](crate::shared_cte::optimize_and_plan_shared_ctes).
    pub fn with_shared_ctes(mut self) -> Self {
        self.share_ctes = self
            .ctx
            .extension::<SharedCteConfig>()
            .is_ok_and(|config| config.enabled());
        self
    }
}
