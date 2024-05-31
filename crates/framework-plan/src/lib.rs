use datafusion::dataframe::DataFrame;
use datafusion::execution::context::QueryPlanner;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{Extension, LogicalPlan};
use std::sync::Arc;

mod catalog;
pub mod error;
mod extension;
pub mod resolver;

/// Execute a logical plan.
/// This replaces DDL statements and catalog operations with the execution results.
/// Logical plan nodes with corresponding physical plan nodes remain unchanged.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    use crate::extension::logical::CatalogCommandNode;

    let plan = match plan {
        LogicalPlan::Extension(Extension { node }) => {
            if let Some(n) = node.as_any().downcast_ref::<CatalogCommandNode>() {
                n.execute(ctx).await?
            } else {
                LogicalPlan::Extension(Extension { node })
            }
        }
        x => x,
    };
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    use crate::extension::ExtensionQueryPlanner;

    Arc::new(ExtensionQueryPlanner {})
}
