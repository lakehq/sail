use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{plan_datafusion_err, Result};
use sail_catalog::error::CatalogResult;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::{CatalogProvider, RuntimeAwareCatalogProvider};
use sail_catalog_iceberg::IcebergRestCatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_catalog_onelake::OneLakeCatalogProvider;
use sail_catalog_system::{SystemCatalogProvider, SYSTEM_CATALOG_NAME};
use sail_catalog_unity::UnityCatalogProvider;
use sail_common::config::{AppConfig, CatalogType};
use sail_common::runtime::RuntimeHandle;
use secrecy::ExposeSecret;

pub fn create_catalog_manager(
    config: &AppConfig,
    runtime: RuntimeHandle,
) -> Result<CatalogManager> {
    let mut catalogs = config
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
                CatalogType::IcebergRest {
                    name,
                    uri,
                    warehouse,
                    prefix,
                    oauth_access_token,
                    bearer_access_token,
                } => {
                    let mut properties = HashMap::new();
                    properties.insert("uri".to_string(), uri.to_string());
                    if let Some(warehouse) = warehouse {
                        properties.insert("warehouse".to_string(), warehouse.to_string());
                    }
                    if let Some(prefix) = prefix {
                        properties.insert("prefix".to_string(), prefix.to_string());
                    }
                    if let Some(oauth_access_token) = oauth_access_token {
                        properties.insert(
                            "oauth-access-token".to_string(), // Iceberg uses kebab-case
                            oauth_access_token.expose_secret().to_string(), // FIXME: Only expose when necessary
                        );
                    }
                    if let Some(bearer_access_token) = bearer_access_token {
                        properties.insert(
                            "bearer-access-token".to_string(), // Iceberg uses kebab-case
                            bearer_access_token.expose_secret().to_string(), // FIXME: Only expose when necessary
                        );
                    }

                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || {
                            let provider =
                                IcebergRestCatalogProvider::new(name.to_string(), properties);
                            Ok(provider)
                        },
                        runtime.io().clone(),
                    )?;

                    Ok((name.to_string(), Arc::new(runtime_aware)))
                }
                CatalogType::Unity {
                    name,
                    uri,
                    default_catalog,
                    token,
                } => {
                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || UnityCatalogProvider::new(name.to_string(), default_catalog, uri, token),
                        runtime.io().clone(),
                    )?;

                    Ok((name.to_string(), Arc::new(runtime_aware)))
                }
                CatalogType::OneLake {
                    name,
                    url,
                    bearer_token,
                } => {
                    // Parse URL format: workspace/item.type (e.g., "duckrun/data.lakehouse", "duckrun/data.datawarehouse")
                    let (workspace, item) = url.split_once('/').ok_or_else(|| {
                        plan_datafusion_err!(
                            "Invalid OneLake URL format: expected 'workspace/item.type', got '{}'",
                            url
                        )
                    })?;

                    // Extract item name and type (e.g., "data.Lakehouse" -> name="data", type="Lakehouse")
                    let (item_name, item_type) = item.split_once('.').ok_or_else(|| {
                        plan_datafusion_err!(
                            "Invalid OneLake item format: expected 'name.type', got '{}'",
                            item
                        )
                    })?;

                    let token = bearer_token.as_ref().map(|t| t.expose_secret().to_string());
                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || {
                            Ok(OneLakeCatalogProvider::new(
                                name.clone(),
                                workspace.to_string(),
                                item_name.to_string(),
                                item_type.to_string(),
                                token.clone(),
                            ))
                        },
                        runtime.io().clone(),
                    )?;

                    Ok((name.to_string(), Arc::new(runtime_aware)))
                }
            }
        })
        .collect::<CatalogResult<HashMap<_, _>>>()
        .map_err(|e| plan_datafusion_err!("failed to create catalog: {e}"))?;
    if catalogs
        .insert(
            SYSTEM_CATALOG_NAME.to_string(),
            Arc::new(SystemCatalogProvider),
        )
        .is_some()
    {
        return Err(plan_datafusion_err!(
            "cannot define catalog with reserved name: {}",
            SYSTEM_CATALOG_NAME
        ));
    }
    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: config.catalog.default_catalog.clone(),
        default_database: config.catalog.default_database.clone(),
        global_temporary_database: config.catalog.global_temporary_database.clone(),
    };
    CatalogManager::new(options)
        .map_err(|e| plan_datafusion_err!("failed to create catalog manager: {e}"))
}
