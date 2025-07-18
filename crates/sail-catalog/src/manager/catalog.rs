use std::sync::Arc;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::CatalogProvider;
use crate::utils::match_pattern;

impl CatalogManager {
    pub fn default_catalog(&self) -> CatalogResult<Arc<str>> {
        Ok(self.state()?.default_catalog.clone())
    }

    pub fn set_default_catalog(&self, catalog: impl Into<Arc<str>>) -> CatalogResult<()> {
        self.state()?.default_catalog = catalog.into();
        Ok(())
    }

    pub fn get_catalog(&self, catalog: &str) -> CatalogResult<Arc<dyn CatalogProvider>> {
        let state = self.state()?;
        let Some(provider) = state.catalogs.get(catalog) else {
            return Err(CatalogError::NotFound("catalog", catalog.to_string()));
        };
        Ok(Arc::clone(provider))
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
}
