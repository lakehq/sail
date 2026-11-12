use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryLimit;
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
    /// Whether the references to a CTE that is not volatile can share one evaluation of it.
    share_deterministic_ctes: bool,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            share_ctes: false,
            share_deterministic_ctes: false,
        }
    }

    /// Resolves the repeated references to a CTE as scans of the CTE, if the session
    /// shares CTEs. The resolved plan must then be optimized with
    /// [`optimize_shared_ctes`](crate::shared_cte::optimize_shared_ctes) and planned by
    /// the query planner of the session.
    pub fn with_shared_ctes(mut self) -> Self {
        self.share_ctes = self
            .ctx
            .extension::<SharedCteConfig>()
            .is_ok_and(|config| config.enabled());
        // A shared CTE is kept in memory when it cannot spill, which other operators may
        // need under a memory limit. Only the CTEs that must be evaluated once are shared.
        let runtime = self.ctx.runtime_env();
        self.share_deterministic_ctes =
            !(matches!(runtime.memory_pool.memory_limit(), MemoryLimit::Finite(_))
                && !runtime.disk_manager.tmp_files_enabled());
        self
    }
}
