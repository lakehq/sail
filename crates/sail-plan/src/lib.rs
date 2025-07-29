use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::utils::rename_physical_plan;
use sail_runtime::RuntimeHandle;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::resolver::plan::NamedPlan;
use crate::resolver::PlanResolver;

pub mod config;
pub mod error;
pub mod extension;
pub mod formatter;
pub mod function;
pub mod literal;
pub mod resolver;
mod utils;

/// Executes a logical plan.
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

pub async fn resolve_and_execute_plan(
    ctx: Arc<SessionContext>,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
    runtime_handle: Option<&RuntimeHandle>,
) -> PlanResult<Arc<dyn ExecutionPlan>> {
    if let Some(handle) = runtime_handle {
        let executor = handle.dedicated_executor().clone();
        executor
            .spawn(async move {
                let resolver = PlanResolver::new(&ctx, config);
                let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
                let df = execute_logical_plan(&ctx, plan).await?;
                let plan = df.create_physical_plan().await?;
                if let Some(fields) = fields {
                    Ok(rename_physical_plan(plan, fields.as_slice())?)
                } else {
                    Ok(plan)
                }
            })
            .await
            .map_err(|e| {
                PlanError::InternalError(format!("failed to execute on CPU runtime: {e}"))
            })?
    } else {
        // CHECK HERE: DO NOT MERGE CODE IF THIS COMMENT IS HERE!!
        Err(PlanError::InternalError(
            "runtime handle is required for plan execution".to_string(),
        ))
    }
}

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    use crate::extension::ExtensionQueryPlanner;

    Arc::new(ExtensionQueryPlanner {})
}
