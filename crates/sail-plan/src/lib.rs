use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::plan::NamedPlan;
use crate::shared_cte::{optimize_and_plan_shared_ctes, session_state_for_restart};
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

pub mod catalog;
pub mod config;
pub mod error;
pub mod explain;
pub mod formatter;
pub mod function;
pub mod procedure;
pub mod resolver;
pub mod scalar_subquery_key;
pub mod shared_cte;
mod streaming;

/// Executes a logical plan.
/// Catalog commands and barrier nodes are handled by the physical planner.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

/// Optimizes a resolved plan with its shared CTEs and creates the physical plan.
pub(crate) async fn plan_with_shared_ctes(
    session_state: &SessionState,
    plan: LogicalPlan,
) -> Result<(LogicalPlan, Arc<dyn ExecutionPlan>)> {
    let mut restart = false;
    optimize_and_plan_shared_ctes(
        plan,
        &mut |plan, query| {
            if query && std::mem::replace(&mut restart, true) {
                session_state_for_restart(session_state).optimize(&plan)
            } else {
                session_state.optimize(&plan)
            }
        },
        |plan| async {
            let plan = if is_streaming_plan(&plan)? {
                rewrite_streaming_plan(plan)?
            } else {
                plan
            };
            let physical = session_state
                .query_planner()
                .create_physical_plan(&plan, session_state)
                .await?;
            Ok((plan, physical))
        },
    )
    .await
}

pub async fn resolve_and_execute_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config).with_shared_ctes();
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let (logical, plan) = plan_with_shared_ctes(&session_state, plan).await?;
    info.push(logical.to_stringified(PlanType::FinalLogicalPlan));
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, &fields)?
    } else {
        plan
    };
    info.push(StringifiedPlan::new(
        PlanType::FinalPhysicalPlan,
        displayable(plan.as_ref()).indent(true).to_string(),
    ));
    Ok((plan, info))
}
