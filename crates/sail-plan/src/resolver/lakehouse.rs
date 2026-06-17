use sail_catalog::lakehouse::ResolveLakehouseTableRequest;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::{
    LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_lakehouse_table_context(
        &self,
        table: &[String],
        operation: LakehouseOperation,
        requested_format: Option<&str>,
        options: Vec<(String, String)>,
    ) -> PlanResult<LakehouseExecutionContext> {
        let resolved = self
            .ctx
            .extension::<CatalogManager>()?
            .resolve_lakehouse_table(
                table,
                ResolveLakehouseTableRequest {
                    catalog_table: table.to_vec(),
                    operation,
                    requested_format: requested_format.map(LakehouseFormat::from_format_name),
                    options,
                },
            )
            .await?;
        Ok(resolved.execution)
    }
}
