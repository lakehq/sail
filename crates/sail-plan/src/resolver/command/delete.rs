use std::sync::Arc;

use datafusion_common::DFSchema;
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
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Delete {
            table,
            table_alias: _,
            condition,
        } = delete;

        // Look up the table in the catalog to get its metadata
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_info = catalog_manager
            .get_table(table.parts())
            .await
            .map_err(|e| PlanError::internal(format!("Failed to get table metadata: {e}")))?;

        let (location, format) = match table_info.kind {
            TableKind::Table {
                location, format, ..
            } => {
                let location = location
                    .ok_or_else(|| PlanError::unsupported("DELETE on tables without location"))?;
                (location, format)
            }
            _ => {
                return Err(PlanError::unsupported(
                    "DELETE is only supported on tables, not views",
                ));
            }
        };

        // Convert the condition expression if present
        let condition = if let Some(condition) = condition {
            // TODO: We need a proper schema for the table to resolve expressions
            Some(
                self.resolve_expression(condition, &Arc::new(DFSchema::empty()), _state)
                    .await?,
            )
        } else {
            None
        };

        let file_delete_options = FileDeleteOptions {
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
