use std::sync::Arc;

use datafusion::dataframe::DataFrame;
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
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

pub mod catalog;
pub mod config;
pub mod error;
pub mod explain;
pub mod formatter;
pub mod function;
pub mod resolver;
mod streaming;

/// Executes a logical plan.
/// Catalog commands and barrier nodes are handled by the physical planner.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub async fn resolve_and_execute_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<Arc<dyn ExecutionPlan>> {
    let (plan, _) = resolve_execution_plan(ctx, config, plan, PlanDescriptionMode::Omit).await?;
    Ok(plan)
}

pub async fn resolve_and_describe_execution_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    resolve_execution_plan(ctx, config, plan, PlanDescriptionMode::Collect).await
}

enum PlanDescriptionMode {
    Omit,
    Collect,
}

async fn resolve_execution_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
    description_mode: PlanDescriptionMode,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let mut descriptions = match description_mode {
        PlanDescriptionMode::Omit => None,
        PlanDescriptionMode::Collect => Some(vec![]),
    };
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    if let Some(descriptions) = &mut descriptions {
        descriptions.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    }
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    if let Some(descriptions) = &mut descriptions {
        descriptions.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    }
    let plan = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await?;
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, &fields)?
    } else {
        plan
    };
    if let Some(descriptions) = &mut descriptions {
        descriptions.push(StringifiedPlan::new(
            PlanType::FinalPhysicalPlan,
            displayable(plan.as_ref()).indent(true).to_string(),
        ));
    }
    Ok((plan, descriptions.unwrap_or_default()))
}
