use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{CatalogObjectHandle, TableHandle};
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_catalog_object_handle(
        &self,
        object: &spec::ObjectName,
    ) -> PlanResult<Option<CatalogObjectHandle>> {
        let manager = self.ctx.extension::<CatalogManager>()?;
        match manager
            .open_table_or_view_handle(object.parts(), &self.ctx.state())
            .await
        {
            Ok(handle) => Ok(Some(handle)),
            Err(CatalogError::NotFound(_, _)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn resolve_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<Option<TableHandle>> {
        match self.resolve_catalog_object_handle(table).await? {
            Some(CatalogObjectHandle::Table(handle)) => Ok(Some(handle)),
            Some(CatalogObjectHandle::View(_)) => Ok(None),
            None => Ok(None),
        }
    }

    pub(super) async fn require_catalog_object_handle(
        &self,
        object: &spec::ObjectName,
    ) -> PlanResult<CatalogObjectHandle> {
        self.resolve_catalog_object_handle(object)
            .await?
            .ok_or_else(|| PlanError::invalid(format!("object does not exist: {object:?}")))
    }

    pub(super) async fn require_table_handle(
        &self,
        table: &spec::ObjectName,
    ) -> PlanResult<TableHandle> {
        match self.require_catalog_object_handle(table).await? {
            CatalogObjectHandle::Table(handle) => Ok(handle),
            CatalogObjectHandle::View(_) => Err(PlanError::invalid(format!(
                "object is not a table: {table:?}"
            ))),
        }
    }
}
