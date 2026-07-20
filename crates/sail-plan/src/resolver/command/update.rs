use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::{Expr, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseOperation, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    UpdateInfo, OptionLayer, SourceInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves the UPDATE command.
    pub(super) async fn resolve_command_update(
        &self,
        update: spec::Update,
        state: &mut PlanResolverState
    ) -> PlanResult<LogicalPlan>  {
        let spec::Update {
            table,
            table_alias: _,
            assignments,
            condition
        } = update;
        let table_name: Vec<String> = table.clone().into();
        // Look up the table in the catalog to get its metadata
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let info = self
            .get_table_info_for_update(&table_status, &table_name)
            .await?;

        let field_ids = state.register_fields(info.schema.fields());

        let original_arrow_schema = std::sync::Arc::new(info.schema.as_arrow().clone());
        let schema_for_resolution = rename_schema(&original_arrow_schema, &field_ids)?;
        let df_schema_for_resolution = schema_for_resolution.to_dfschema_ref()?;

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            let resolved_condition = self
                .resolve_expression(condition, &df_schema_for_resolution, state)
                .await?;
            let rewritten_condition = expression_before_rename(
                &resolved_condition,
                &field_ids,
                &original_arrow_schema,
                true
            )?;
            Some(ExprWithSource::new(rewritten_condition, None))
        } else {
            None
        };

        // Convert the assignments
        let mut resolved_assignments = Vec::with_capacity(assignments.len());
        for (column_name, value) in assignments {
            let expr = spec::Expr::UnresolvedAttribute {
                name: column_name,
                plan_id: None,
                is_metadata_column: false
            };
            let resolved_col = self
                .resolve_expression(expr, &df_schema_for_resolution, state)
                .await?;
            let rewritten_col = expression_before_rename(
                &resolved_col,
                &field_ids,
                &original_arrow_schema,
                true
            )?;
            let column_name = match rewritten_col {
                Expr::Column(col) => col.name,
                _ => return Err(PlanError::invalid("UPDATE assignments must reference columns only"))
            };
            if resolved_assignments.iter().any(|(c, _)| c == &column_name) {
                return Err(PlanError::invalid(format!(
                    "conflicting assignments found for SET column `{column_name}`"
                )))
            }

            let resolved_value = self
                .resolve_expression(value, &df_schema_for_resolution, state)
                .await?;
            let rewritten_value = expression_before_rename(
                &resolved_value,
                &field_ids,
                &original_arrow_schema,
                true
            )?;

            resolved_assignments.push((column_name, rewritten_value));
        }

        let update_info = UpdateInfo {
            table_name,
            path: info.location,
            condition,
            assignments: resolved_assignments,
            lakehouse_table: info.lakehouse_table,
            options: vec![OptionLayer::TablePropertyList {
                items: info.properties
            }]
        };

        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        registry
            .get(&info.format)?
            .create_updater(&self.ctx.state(), update_info)
            .await
            .map_err(PlanError::from)
    }

    async fn get_table_info_for_update(
        &self,
        table_status: &TableStatus,
        table_name: &[String]
    ) -> PlanResult<TableInfo>  {
        let (location, format, columns, properties) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                columns,
                properties,
                ..
            } => (
                location.clone(),
                format.clone(),
                columns.clone(),
                properties.clone()
            ),
            _ => {
                return Err(PlanError::unsupported(
                    "UPDATE is only supported on tables, not views",
                ))
            }
        };

        let location =
            location.ok_or_else(|| PlanError::unsupported("UPDATE on tables without location"))?;
        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                table_name,
                LakehouseOperation::Read,
                Some(&format),
                vec![]
            )
            .await?;

        let schema = if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") {
            // Schema is not in catalog, try to infer from data source
            let source_info = SourceInfo {
                paths: vec![location.clone()],
                lakehouse_table: Some(lakehouse_table.clone()),
                schema: None,
                constraints: Default::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: vec![],
                read_case_sensitive: self.config.case_sensitive
            };
            let registry = self.ctx.extension::<TableFormatRegistry>()?;
            let table_format = registry.get(&format)?;
            let source = table_format
                .create_source(&self.ctx.state(), source_info)
                .await?;
            source.schema().to_dfschema_ref()?
        } else {
            let schema = datafusion::arrow::datatypes::Schema::new(
                columns.iter().map(|c| c.field()).collect::<Vec<_>>()
            );
            schema.to_dfschema_ref()?
        };

        Ok(TableInfo {
            location,
            format,
            schema,
            properties,
            lakehouse_table: Some(lakehouse_table.for_operation(LakehouseOperation::Write))
        })
    }


}

struct TableInfo {
    location: String,
    format: String,
    schema: DFSchemaRef,
    properties: Vec<(String, String)>,
    lakehouse_table: Option<LakehouseExecutionContext>
}
