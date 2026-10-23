use sail_common::spec;
pub use sail_query::ir::*;

#[derive(Debug, Clone)]
pub struct LogicalQuery {
    pub plan: LogicalPlan,
}

impl From<spec::QueryPlan> for LogicalQuery {
    fn from(plan: spec::QueryPlan) -> Self {
        Self {
            plan: LogicalPlan::unresolved(plan),
        }
    }
}
