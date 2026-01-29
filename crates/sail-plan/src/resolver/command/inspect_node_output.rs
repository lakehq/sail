use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::DFSchema;
use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;

use crate::error::PlanResult;
use crate::inspect::{infer_node_schema, InspectNodeOutputNode};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion]
    pub(super) async fn resolve_command_inspect_node_output(
        &self,
        node: String,
        format: spec::InspectNodeOutputFormat,
        input: spec::Plan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = match input {
            spec::Plan::Query(query) => self.resolve_query_plan(query, state).await?,
            spec::Plan::Command(command) => self.resolve_command_plan(command, state).await?,
        };
        let schema = match format {
            spec::InspectNodeOutputFormat::Pretty => InspectNodeOutputNode::pretty_schema()?,
            spec::InspectNodeOutputFormat::Raw => {
                let schema = infer_node_schema(self.ctx, plan.clone(), &node).await?;
                Arc::new(DFSchema::try_from(schema.as_ref().clone())?)
            }
        };
        let node = InspectNodeOutputNode::try_new(node, format, schema, plan)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }
}
