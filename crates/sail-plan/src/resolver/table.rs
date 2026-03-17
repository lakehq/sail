use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableHandle;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<Option<TableHandle>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        let handle = match manager
            .open_table_handle(table.parts(), &self.ctx.state())
            .await
        {
            Ok(handle) => handle,
            Err(CatalogError::NotFound(_, _)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(handle))
    }

    pub(super) async fn require_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<TableHandle> {
        self.resolve_table_handle(table)
            .await?
            .ok_or_else(|| PlanError::invalid(format!("table does not exist: {table:?}")))
    }
}
