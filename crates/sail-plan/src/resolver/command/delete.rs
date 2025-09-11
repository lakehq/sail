use std::sync::Arc;

use datafusion_common::ToDFSchema;
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::TableKind;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;

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

        let (location, format, table_schema) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                columns,
                ..
            } => {
                let location = location
                    .clone()
                    .ok_or_else(|| PlanError::unsupported("DELETE on tables without location"))?;
                let schema = datafusion::arrow::datatypes::Schema::new(
                    columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
                );
                (location, format.clone(), schema.to_dfschema_ref()?)
            }
            _ => {
                return Err(PlanError::unsupported(
                    "DELETE is only supported on tables, not views",
                ));
            }
        };

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            Some(
                self.resolve_expression(condition, &table_schema, state)
                    .await?,
            )
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
}
