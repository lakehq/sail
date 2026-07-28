use std::sync::Arc;

use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::LogicalPlan;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseOperation, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    DeleteInfo, OptionLayer, SourceInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_common_datafusion::rename::expression::expression_before_rename;
use sail_common_datafusion::rename::schema::rename_schema;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

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
        let table_name: Vec<String> = table.clone().into();
        // Look up the table in the catalog to get its metadata
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(table.parts())
            .await
            .map_err(PlanError::from)?;
        let info = self
            .get_table_info_for_delete(&table_status, &table_name)
            .await?;

        let field_ids = state.register_fields(info.schema.fields());

        let original_arrow_schema = Arc::new(info.schema.as_arrow().clone());
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

        let delete_info = DeleteInfo {
            table_name,
            path: info.location,
            condition,
            lakehouse_table: info.lakehouse_table,
            options: vec![OptionLayer::TablePropertyList {
                items: info.properties,
            }],
        };

        let registry = self.ctx.extension::<TableFormatRegistry>()?;
        registry
            .get(&info.format)?
            .create_deleter(&self.ctx.state(), delete_info)
            .await
            .map_err(PlanError::from)
    }

    async fn get_table_info_for_delete(
        &self,
        table_status: &TableStatus,
        table_name: &[String],
    ) -> PlanResult<TableInfo> {
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
                properties.clone(),
            ),
            _ => {
                return Err(PlanError::unsupported(
                    "DELETE is only supported on tables, not views",
                ));
            }
        };

        let location =
            location.ok_or_else(|| PlanError::unsupported("DELETE on tables without location"))?;
        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                table_name,
                LakehouseOperation::Read,
                Some(&format),
                vec![],
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
                read_case_sensitive: self.config.case_sensitive,
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

        Ok(TableInfo {
            location,
            format,
            schema,
            properties,
            lakehouse_table: Some(lakehouse_table.for_operation(LakehouseOperation::Write)),
        })
    }
}

struct TableInfo {
    location: String,
    format: String,
    schema: DFSchemaRef,
    properties: Vec<(String, String)>,
    lakehouse_table: Option<LakehouseExecutionContext>,
}
