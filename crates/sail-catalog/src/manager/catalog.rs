use std::sync::Arc;

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::CatalogProvider;
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

    /// Registers a catalog for the current session, or replaces a catalog that the session
    /// registered earlier. A catalog defined by the server configuration is never replaced.
    pub fn register_session_catalog(
        &self,
        name: impl Into<Arc<str>>,
        provider: Arc<dyn CatalogProvider>,
    ) -> CatalogResult<()> {
        let name = name.into();
        let mut state = self.state()?;
        if state.catalogs.contains_key(&name) && !state.session_catalogs.contains(&name) {
            return Err(CatalogError::AlreadyExists(
                CatalogObject::Catalog,
                name.to_string(),
            ));
        }
        state.session_catalogs.insert(name.clone());
        state.catalogs.insert(name, provider);
        Ok(())
    }

    /// Returns whether the name is taken by a catalog that the session did not register,
    /// such as a catalog defined by the server configuration.
    pub fn has_fixed_catalog(&self, name: &str) -> CatalogResult<bool> {
        let state = self.state()?;
        Ok(state.catalogs.contains_key(name) && !state.session_catalogs.contains(name))
    }

    /// Removes a catalog that the session registered. This does nothing for a name that the
    /// session did not register, so the catalogs defined by the server configuration stay.
    /// The default catalog cannot be removed.
    pub fn remove_session_catalog(&self, name: &str) -> CatalogResult<()> {
        let mut state = self.state()?;
        if !state.session_catalogs.contains(name) {
            return Ok(());
        }
        if state.default_catalog.as_ref() == name {
            return Err(CatalogError::InvalidArgument(format!(
                "cannot remove the default catalog: {name}"
            )));
        }
        state.session_catalogs.remove(name);
        state.catalogs.remove(name);
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

    /// Resolves an explicit or default catalog name and verifies that it exists.
    pub fn resolve_catalog_reference(&self, catalog: Option<&str>) -> CatalogResult<Arc<str>> {
        let state = self.state()?;
        let catalog = catalog
            .map(Arc::<str>::from)
            .unwrap_or_else(|| state.default_catalog.clone());
        state.get_catalog(&catalog)?;
        Ok(catalog)
    }
}
