use crate::sql::session_catalog::{SessionCatalogContext, SessionContextExt};
use crate::sql::utils::match_pattern;
use datafusion_common::Result;
use framework_common::unwrap_or;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DatabaseMetadata {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) location_uri: Option<String>,
}

impl SessionCatalogContext<'_> {
    pub(crate) fn default_database(&self) -> Result<String> {
        Ok(self
            .ctx
            .read_state(|state| Ok(state.config().options().catalog.default_schema.clone()))?)
    }

    pub(crate) fn set_default_database(&self, database_name: String) -> Result<()> {
        self.ctx.write_state(move |state| {
            state.config_mut().options_mut().catalog.default_schema = database_name;
            Ok(())
        })
    }

    pub(crate) fn list_databases(
        &self,
        catalog_pattern: Option<&str>,
        database_pattern: Option<&str>,
    ) -> Result<Vec<DatabaseMetadata>> {
        let mut databases: Vec<DatabaseMetadata> = Vec::new();
        for catalog in self.list_catalogs(catalog_pattern)? {
            let catalog_provider = unwrap_or!(self.ctx.catalog(&catalog.name), continue);
            for database_name in catalog_provider.schema_names() {
                if match_pattern(&database_name, database_pattern) {
                    databases.push(DatabaseMetadata {
                        name: database_name,
                        catalog: Some(catalog.name.clone()),
                        description: None, // TODO: Add actual description if available
                        location_uri: None, // TODO: Add actual location URI if available
                    })
                }
            }
        }
        Ok(databases)
    }

    pub(crate) fn infer_database(&self, database_name: Option<String>) -> Result<String> {
        match database_name {
            Some(x) => Ok(x),
            None => Ok(self.default_database()?),
        }
    }
}
