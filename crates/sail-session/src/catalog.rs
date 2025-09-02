use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{plan_datafusion_err, Result};
use sail_catalog::error::CatalogResult;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::CatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::config::{AppConfig, CatalogType};

pub fn create_catalog_manager(config: &AppConfig) -> Result<CatalogManager> {
    let catalogs = config
        .catalog
        .list
        .iter()
        .map(|x| -> CatalogResult<(String, Arc<dyn CatalogProvider>)> {
            match x {
                CatalogType::Memory {
                    name,
                    initial_database,
                    initial_database_comment,
                } => {
                    let provider = MemoryCatalogProvider::new(
                        name.clone(),
                        initial_database.clone().try_into()?,
                        initial_database_comment.clone(),
                    );
                    Ok((name.clone(), Arc::new(provider)))
                }
            }
        })
        .collect::<CatalogResult<HashMap<_, _>>>()
        .map_err(|e| plan_datafusion_err!("failed to create catalog: {e}"))?;
    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: config.catalog.default_catalog.clone(),
        default_database: config.catalog.default_database.clone(),
        global_temporary_database: config.catalog.global_temporary_database.clone(),
    };
    CatalogManager::new(options)
        .map_err(|e| plan_datafusion_err!("failed to create catalog manager: {e}"))
}
