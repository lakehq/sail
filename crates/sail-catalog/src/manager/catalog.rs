use std::sync::Arc;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::utils::match_pattern;

impl CatalogManager {
    pub fn default_catalog(&self) -> CatalogResult<Arc<str>> {
        Ok(self.state()?.default_catalog.clone())
    }

    /// Sets the default catalog for the current session.
    /// An error is returned if the catalog does not exist.
    pub fn set_default_catalog(&self, catalog: impl Into<Arc<str>>) -> CatalogResult<()> {
        let catalog = catalog.into();
        let mut state = self.state()?;
        if !state.catalogs.contains_key(&catalog) {
            return Err(CatalogError::NotFound(
                CatalogObject::Catalog,
                catalog.to_string(),
            ));
        }
        state.default_catalog = catalog;
        Ok(())
    }

    pub fn list_catalogs(&self, pattern: Option<&str>) -> CatalogResult<Vec<Arc<str>>> {
        Ok(self
            .state()?
            .catalogs
            .keys()
            .filter(|name| match_pattern(name.as_ref(), pattern))
            .cloned()
            .collect::<Vec<_>>())
    }

    /// Resolves the catalog that owns a procedure name and returns the lake sources it exposes.
    pub fn resolve_procedure_catalog(
        &self,
        catalog: Option<&str>,
    ) -> CatalogResult<(Arc<str>, Vec<String>)> {
        let state = self.state()?;
        let catalog = catalog
            .map(Arc::<str>::from)
            .unwrap_or_else(|| state.default_catalog.clone());
        let provider = state.get_catalog(&catalog)?;
        let lake_sources = provider.lake_procedure_sources();
        if lake_sources.is_empty() {
            return Err(CatalogError::UnsupportedCapability(format!(
                "catalog '{catalog}' does not support procedures"
            )));
        }
        Ok((catalog, lake_sources))
    }
}
