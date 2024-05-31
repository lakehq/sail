use std::sync::Arc;

use crate::catalog::utils::match_pattern;
use crate::catalog::{CatalogContext, SessionContextExt};
use datafusion::catalog::CatalogProviderList;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CatalogMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
}

impl CatalogMetadata {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            description: None, // Spark code sets all descriptions to None
        }
    }
}

impl CatalogContext<'_> {
    pub(crate) fn default_catalog(&self) -> Result<String> {
        Ok(self
            .ctx
            .read_state(|state| Ok(state.config().options().catalog.default_catalog.clone()))?)
    }

    pub(crate) fn set_default_catalog(&self, catalog_name: String) -> Result<()> {
        self.ctx.write_state(move |state| {
            state.config_mut().options_mut().catalog.default_catalog = catalog_name;
            Ok(())
        })
    }

    pub(crate) fn list_catalogs(
        &self,
        catalog_pattern: Option<&str>,
    ) -> Result<Vec<CatalogMetadata>> {
        let catalog_list: Arc<dyn CatalogProviderList> =
            self.ctx.read_state(|state| Ok(state.catalog_list()))?;
        Ok(catalog_list
            .catalog_names()
            .into_iter()
            .filter(|name| match_pattern(name.as_str(), catalog_pattern))
            .map(|name| CatalogMetadata::new(name))
            .collect::<Vec<_>>())
    }
}
