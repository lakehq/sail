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
use sail_logical_plan::file_update::{FileUpdateNode, FileUpdateOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the UPDATE command.
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let _ = table_alias;
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let (location, format, table_schema) = self.get_schema_for_update(&table_status).await?;

        let field_ids = state.register_fields(table_schema.fields());

        let original_arrow_schema = Arc::new(table_schema.as_arrow().clone());
        let schema_for_resolution = rename_schema(&original_arrow_schema, &field_ids)?;
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        // Resolve each assignment expression
        let resolved_assignments = {
            let mut result = vec![];
            for (col_name, expr) in assignments {
                // Use the last part of the column name (handles qualified names like `alias.col`)
                let col = col_name
                    .parts()
                    .last()
                    .ok_or_else(|| PlanError::invalid("empty column name in UPDATE assignment"))?
                    .as_ref()
                    .to_string();
                let resolved_expr = self
                    .resolve_expression(expr, &df_schema_for_resolution, state)
                    .await?;
                let rewritten_expr = expression_before_rename(
                    &resolved_expr,
                    &field_ids,
                    &original_arrow_schema,
                    true,
                )?;
                result.push((col, ExprWithSource::new(rewritten_expr, None)));
            }
            result
        };

        // Resolve the WHERE condition if present
        let resolved_condition = if let Some(condition) = condition {
            let resolved = self
                .resolve_expression(condition, &df_schema_for_resolution, state)
                .await?;
            let rewritten =
                expression_before_rename(&resolved, &field_ids, &original_arrow_schema, true)?;
            Some(ExprWithSource::new(rewritten, None))
        } else {
            None
        };

        let file_update_options = FileUpdateOptions {
            table_name: table.into(),
            path: location,
            format,
            condition: resolved_condition,
            assignments: resolved_assignments,
            options: vec![],
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FileUpdateNode::new(file_update_options)),
        }))
    }

    async fn get_schema_for_update(
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
                    "UPDATE is only supported on tables, not views",
                ));
            }
        };

        let location =
            location.ok_or_else(|| PlanError::unsupported("UPDATE on tables without location"))?;

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
            let source = table_format
                .create_source(&self.ctx.state(), source_info)
                .await?;
            source.schema().to_dfschema_ref()?
        } else {
            let schema = datafusion::arrow::datatypes::Schema::new(
                columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
            );
            schema.to_dfschema_ref()?
        };

        Ok((location, format, schema))
    }
}
