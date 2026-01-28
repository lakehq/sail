use std::sync::Arc;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
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
        // Look up the table in the catalog to get its metadata
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let (location, format, table_schema) = self.get_schema_for_delete(&table_status).await?;

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
            table_name: table.into(),
            path: location,
            format,
            condition,
            options: vec![],
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileDeleteNode::new(file_delete_options)),
        }))
    }

    async fn get_schema_for_delete(
        &self,
        table_status: &TableStatus,
    ) -> PlanResult<(String, String, DFSchemaRef)> {
        let (location, format, columns) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                columns,
                ..
            } => (location.clone(), format.clone(), columns.clone()),
            _ => {
                return Err(PlanError::unsupported(
                    "DELETE is only supported on tables, not views",
                ));
            }
        };

        let location =
            location.ok_or_else(|| PlanError::unsupported("DELETE on tables without location"))?;

        let schema = if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") {
            // Schema is not in catalog, try to infer from data source
            let source_info = SourceInfo {
                paths: vec![location.clone()],
                schema: None,
                constraints: Default::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: vec![],
            };
            let registry = self.ctx.extension::<TableFormatRegistry>()?;
            let table_format = registry.get(&format)?;
            let provider = table_format
                .create_provider(&self.ctx.state(), source_info)
                .await?;
            provider.schema().to_dfschema_ref()?
        } else {
            let schema = datafusion::arrow::datatypes::Schema::new(
                columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
            );
            schema.to_dfschema_ref()?
        };

        Ok((location, format, schema))
    }
}
