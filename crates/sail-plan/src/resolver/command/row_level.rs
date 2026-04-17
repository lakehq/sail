use datafusion_common::{DFSchemaRef, ToDFSchema};
use sail_common_datafusion::catalog::{TableKind, TableStatus};
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Shared helper that resolves `(location, format, schema)` for a row-level
    /// operation (DELETE/UPDATE) target. `op_name` is used only for error messages.
    pub(super) async fn get_row_level_target_schema(
        &self,
        table_status: &TableStatus,
        op_name: &str,
    ) -> PlanResult<(String, String, DFSchemaRef)> {
        let (location, format, columns) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                columns,
                ..
            } => (location.clone(), format.clone(), columns.clone()),
            _ => {
                return Err(PlanError::unsupported(format!(
                    "{op_name} is only supported on tables, not views"
                )));
            }
        };

        let location = location.ok_or_else(|| {
            PlanError::unsupported(format!("{op_name} on tables without location"))
        })?;

        let schema = if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") {
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
