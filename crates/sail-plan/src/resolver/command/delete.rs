use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{
    tree_node::{Transformed, TreeNode},
    Column, DFSchemaRef, ToDFSchema,
};
use datafusion_expr::{Expr, Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{TableKind, TableStatus};
use sail_common::spec;
use sail_common_datafusion::datasource::SourceInfo;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_data_source::default_registry;

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::{FileDeleteNode, FileDeleteOptions};
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

        let fields_with_ids: Vec<_> = table_schema
            .fields()
            .iter()
            .zip(field_ids.iter())
            .map(|(field, field_id)| {
                datafusion_common::arrow::datatypes::Field::new(
                    field_id,
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        let schema_for_resolution =
            datafusion_common::arrow::datatypes::Schema::new(fields_with_ids);
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            let resolved_condition = self
                .resolve_expression(condition, &df_schema_for_resolution, state)
                .await?;

            let field_id_to_name: HashMap<String, String> = field_ids
                .iter()
                .zip(table_schema.fields().iter())
                .map(|(field_id, field)| (field_id.clone(), field.name().to_string()))
                .collect();

            let rewritten_condition =
                rewrite_field_ids_to_names(resolved_condition, &field_id_to_name)?;

            Some(rewritten_condition)
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
            let table_format = default_registry().get_format(&format)?;
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

/// Rewrite an expression to replace field IDs with actual field names
fn rewrite_field_ids_to_names(
    expr: Expr,
    field_id_to_name: &HashMap<String, String>,
) -> PlanResult<Expr> {
    expr.transform(&|expr| match &expr {
        Expr::Column(Column { name, .. }) => {
            if let Some(actual_name) = field_id_to_name.get(name) {
                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    actual_name,
                ))))
            } else {
                Ok(Transformed::no(expr))
            }
        }
        _ => Ok(Transformed::no(expr)),
    })
    .map(|t| t.data)
    .map_err(|e| PlanError::internal(format!("Failed to rewrite expression: {e}")))
}
