use std::sync::Arc;

use crate::sql::session_catalog::{SessionCatalogContext, SessionContextExt};
use crate::sql::utils::match_pattern;
use datafusion::catalog::CatalogProviderList;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CatalogMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
}

impl SessionCatalogContext<'_> {
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
        let mut catalogs: Vec<CatalogMetadata> = Vec::new();
        for catalog_name in catalog_list.catalog_names() {
            if !match_pattern(&catalog_name, catalog_pattern) {
                continue;
            }
            catalogs.push(CatalogMetadata {
                name: catalog_name,
                description: None, // Spark code sets all descriptions to None
            })
        }
        Ok(catalogs)
    }
}
