use std::sync::Arc;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::catalog::TableHandle;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;
use sail_logical_plan::file_delete::{FileDeleteNode, FileDeleteOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the DELETE command.
    pub(super) async fn resolve_command_delete(
        &self,
        delete: spec::Delete,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Delete {
            table,
            table_alias: _,
            condition,
        } = delete;
        let table_handle = self.require_table_handle(&table).await?;
        let table_schema = self.get_schema_for_delete(&table_handle)?;

        let field_ids = state.register_fields(table_schema.fields());

        let original_arrow_schema = Arc::new(table_schema.as_arrow().clone());
        let schema_for_resolution = rename_schema(&original_arrow_schema, &field_ids)?;
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            let resolved_condition = self
                .resolve_expression(condition.expr, &df_schema_for_resolution, state)
                .await?;

            let rewritten_condition = expression_before_rename(
                &resolved_condition,
                &field_ids,
                &original_arrow_schema,
                true,
            )?;

            Some(ExprWithSource::new(rewritten_condition, condition.source))
        } else {
            None
        };

        let file_delete_options = FileDeleteOptions {
            table: table_handle,
            condition,
            options: vec![],
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileDeleteNode::new(file_delete_options)),
        }))
    }

    fn get_schema_for_delete(&self, table: &TableHandle) -> PlanResult<DFSchemaRef> {
        if table.location().is_none() {
            return Err(PlanError::unsupported("DELETE on tables without location"));
        }
        Ok(table.schema().to_dfschema_ref()?)
    }
}
