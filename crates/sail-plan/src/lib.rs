use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::LogicalPlan;
use sail_cache::file_caches::FileCaches;
use sail_cache::invalidation::FileCacheInvalidation;
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_data_source::listing::write::FileWriteNode;

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
) -> PlanResult<(
    Arc<dyn ExecutionPlan>,
    Vec<StringifiedPlan>,
    FileCacheInvalidation,
)> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    info.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    let mut write_paths = Vec::new();
    if FileCaches::from_config(session_state.config())
        .listing
        .is_some()
    {
        plan.apply(|node| {
            if matches!(node, LogicalPlan::Explain(_)) {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let LogicalPlan::Extension(extension) = node
                && let Some(write) = extension.node.as_any().downcast_ref::<FileWriteNode>()
            {
                write_paths.push(ListingTableUrl::try_new(write.options().url.clone(), None)?);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let invalidation = FileCacheInvalidation::new(ctx, &write_paths)?;
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
    Ok((plan, info, invalidation))
}
