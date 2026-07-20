use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_common_datafusion::rename::schema::rename_schema;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
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
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let (plan, info, _) = resolve_plan_execution(ctx, config, plan).await?;
    Ok((plan, info))
}

pub async fn resolve_and_execute_query(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    query: spec::QueryPlan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>, SchemaRef)> {
    let (plan, info, schema) =
        resolve_plan_execution(ctx, config, spec::Plan::Query(query)).await?;
    let schema = schema.ok_or_else(|| PlanError::internal("query output schema is missing"))?;
    Ok((plan, info, schema))
}

async fn resolve_plan_execution(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(
    Arc<dyn ExecutionPlan>,
    Vec<StringifiedPlan>,
    Option<SchemaRef>,
)> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let output_schema = fields
        .as_ref()
        .map(|names| -> Result<SchemaRef> {
            let schema = plan.schema().inner();
            let renamed = rename_schema(schema, names)?;
            Ok(Arc::new(Schema::new_with_metadata(
                renamed.fields().clone(),
                schema.metadata().clone(),
            )))
        })
        .transpose()?;
    let plan = if is_streaming_plan(&plan)? {
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
    Ok((plan, info, output_schema))
}
