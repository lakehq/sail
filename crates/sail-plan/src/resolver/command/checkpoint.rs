use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_logical_plan::remote_checkpoint::RemoteCheckpointCommandNode;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_remote_checkpoint(
        &self,
        relation_id: String,
        input: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let names = Self::get_field_names(input.schema(), state)?;
        let input_schema = input.schema().inner();
        let renamed = rename_schema(input_schema, &names)?;
        let logical_schema = Arc::new(Schema::new_with_metadata(
            renamed.fields().clone(),
            input_schema.metadata().clone(),
        ));
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteCheckpointCommandNode::new(
                relation_id,
                Arc::new(input),
                logical_schema,
            )),
        }))
    }
}
