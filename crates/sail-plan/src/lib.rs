use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_lineage::{LineagePlan, LineageService};

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
pub mod procedure;
pub mod resolver;
mod streaming;

/// Executes a logical plan.
/// Catalog commands and barrier nodes are handled by the physical planner.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

/// A physical plan ready for execution, along with information about planning.
pub struct ExecutablePlan {
    pub plan: Arc<dyn ExecutionPlan>,
    /// The stringified plans at each planning stage.
    pub info: Vec<StringifiedPlan>,
    /// The datasets read and written by the plan.
    pub lineage: LineagePlan,
}

pub async fn resolve_and_execute_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let ExecutablePlan { plan, info, .. } =
        resolve_and_execute_plan_with_lineage(ctx, config, plan).await?;
    Ok((plan, info))
}

/// Resolves the plan and creates the physical plan, while collecting
/// the lineage information of the plan.
pub async fn resolve_and_execute_plan_with_lineage(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<ExecutablePlan> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    let outputs = resolver.take_lineage_outputs();
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let streaming = is_streaming_plan(&plan)?;
    // The inputs are found before the streaming rewrite, which may wrap the data sources.
    let lineage = match ctx.extension::<LineageService>() {
        Ok(service) => LineagePlan {
            operation: outputs.operation.unwrap_or_default(),
            inputs: service.find_inputs(&plan),
            outputs: outputs.datasets,
            streaming,
        },
        _ => LineagePlan::default(),
    };
    let plan = if streaming {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    info.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    let plan = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await?;
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, &fields)?
    } else {
        plan
    };
    info.push(StringifiedPlan::new(
        PlanType::FinalPhysicalPlan,
        displayable(plan.as_ref()).indent(true).to_string(),
    ));
    Ok(ExecutablePlan {
        plan,
        info,
        lineage,
    })
}
