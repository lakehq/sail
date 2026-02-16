use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::Result;
use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::cache_manager::CacheManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_logical_plan::in_memory_relation::InMemoryRelationNode;
use sail_logical_plan::precondition::WithPreconditionsNode;

use crate::catalog::CatalogCommandNode;
use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::plan::NamedPlan;
use crate::resolver::PlanResolver;
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
/// This replaces DDL statements and catalog operations with the execution results.
/// Logical plan nodes with corresponding physical plan nodes remain unchanged.
#[async_recursion]
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let plan = match plan {
        LogicalPlan::Extension(Extension { node }) => {
            if let Some(n) = node.as_any().downcast_ref::<CatalogCommandNode>() {
                n.execute(ctx).await?
            } else if let Some(n) = node.as_any().downcast_ref::<WithPreconditionsNode>() {
                for plan in n.preconditions() {
                    let _ = execute_logical_plan(ctx, plan.as_ref().clone()).await?;
                }
                n.plan().clone()
            } else {
                LogicalPlan::Extension(Extension { node })
            }
        }
        x => x,
    };
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

/// Resolves a spec plan into a physical execution plan.
///
/// Converts the input plan through logical resolution, cache substitution,
/// optimization, and physical planning. Does not execute the resulting plan.
pub async fn resolve_to_execution_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));

    let plan = if let Ok(cache) = ctx.extension::<CacheManager>() {
        use_cached_data(&cache, plan)?
    } else {
        plan
    };

    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
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
    Ok((plan, info))
}

/// Replaces cached subtrees with InMemoryRelation nodes.
///
/// Walks the plan top-down (including subquery expressions) and substitutes any
/// subtree matching a cached entry with an InMemoryRelationNode carrying the cache ID.
/// Equivalent to Spark's `CacheManager.useCachedData`:
/// `spark/sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala:496`
fn use_cached_data(cache: &CacheManager, plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_down_with_subqueries(|node| {
        let Some(cached) = cache.find_match(&node) else {
            return Ok(Transformed::no(node));
        };
        let relation =
            InMemoryRelationNode::new(cached.plan.schema().clone(), cached.cache_id.clone());
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(relation),
        })))
    })
    .map(|t| t.data)
}
