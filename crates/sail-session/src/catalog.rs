use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{plan_datafusion_err, Result};
use datafusion_common::plan_err;
use sail_catalog::error::CatalogResult;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::{
    CachingCatalogProvider, CatalogCacheManager, CatalogProvider, RuntimeAwareCatalogProvider,
};
use sail_catalog_glue::{GlueCatalogConfig, GlueCatalogProvider};
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};
use sail_catalog_iceberg::IcebergRestCatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_catalog_onelake::OneLakeCatalogProvider;
use sail_catalog_system::{SystemCatalogProvider, SYSTEM_CATALOG_NAME};
use sail_catalog_unity::UnityCatalogProvider;
use sail_common::config::{AppConfig, CacheType, CatalogCacheConfig, CatalogType};
use sail_common::runtime::RuntimeHandle;
use secrecy::ExposeSecret;

pub fn create_catalog_manager(
    config: &AppConfig,
    runtime: RuntimeHandle,
    cache_manager: Arc<CatalogCacheManager>,
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
                    let warehouse_dir =
                        sail_plan::config::qualify_warehouse_directory("spark-warehouse");
                    let provider = MemoryCatalogProvider::new(
                        name.clone(),
                        initial_database.clone().try_into()?,
                        initial_database_comment.clone(),
                        Some(warehouse_dir),
                    );
                    Ok((name.clone(), Arc::new(provider)))
                }
                CatalogType::IcebergRest {
                    name,
                    uri,
                    warehouse,
                    prefix,
                    namespace_separator,
                    oauth_access_token,
                    bearer_access_token,
                    cache,
                } => {
                    let mut properties = HashMap::new();
                    properties.insert("uri".to_string(), uri.to_string());
                    if let Some(warehouse) = warehouse {
                        properties.insert("warehouse".to_string(), warehouse.to_string());
                    }
                    if let Some(prefix) = prefix {
                        properties.insert("prefix".to_string(), prefix.to_string());
                    }
                    if let Some(namespace_separator) = namespace_separator {
                        properties.insert(
                            "namespace-separator".to_string(),
                            namespace_separator.to_string(),
                        );
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
                    let provider = wrap_catalog_provider(
                        Arc::new(runtime_aware),
                        name,
                        cache,
                        &cache_manager,
                    )?;
                    Ok((name.to_string(), provider))
                }
                CatalogType::Unity {
                    name,
                    uri,
                    default_catalog,
                    token,
                    cache,
                } => {
                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || UnityCatalogProvider::new(name.to_string(), default_catalog, uri, token),
                        runtime.io().clone(),
                    )?;
                    let provider = wrap_catalog_provider(
                        Arc::new(runtime_aware),
                        name,
                        cache,
                        &cache_manager,
                    )?;
                    Ok((name.to_string(), provider))
                }
                CatalogType::OneLake {
                    name,
                    url,
                    bearer_token,
                    cache,
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
                    let provider = wrap_catalog_provider(
                        Arc::new(runtime_aware),
                        name,
                        cache,
                        &cache_manager,
                    )?;
                    Ok((name.to_string(), provider))
                }
                CatalogType::Glue {
                    name,
                    region,
                    endpoint_url,
                    cache,
                } => {
                    let config = GlueCatalogConfig {
                        region: region.clone(),
                        endpoint_url: endpoint_url.clone(),
                    };
                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || Ok(GlueCatalogProvider::new(name.to_string(), config)),
                        runtime.io().clone(),
                    )?;
                    let provider = wrap_catalog_provider(
                        Arc::new(runtime_aware),
                        name,
                        cache,
                        &cache_manager,
                    )?;
                    Ok((name.to_string(), provider))
                }
                CatalogType::HiveMetastore {
                    name,
                    uris,
                    thrift_transport,
                    auth,
                    kerberos_service_principal,
                    min_sasl_qop,
                    connect_timeout_secs,
                    cache,
                } => {
                    let config = HmsCatalogConfig {
                        uris: uris.clone(),
                        thrift_transport: thrift_transport.clone(),
                        auth: auth.clone(),
                        kerberos_service_principal: kerberos_service_principal.clone(),
                        min_sasl_qop: min_sasl_qop.clone(),
                        connect_timeout_secs: *connect_timeout_secs,
                    };
                    let provider =
                        HmsCatalogProvider::try_new(name.to_string(), config, runtime.clone())?;
                    let provider =
                        wrap_catalog_provider(Arc::new(provider), name, cache, &cache_manager)?;
                    Ok((name.to_string(), provider))
                }
            }
        })
        .collect::<CatalogResult<HashMap<_, _>>>()
        .map_err(|e| plan_datafusion_err!("failed to create catalog: {e}"))?;
    let default_catalog = if let Some(name) = config.catalog.default_catalog.as_ref() {
        name.clone()
    } else {
        let mut keys = catalogs.keys();
        if let Some(name) = keys.next() {
            if keys.next().is_none() {
                name.clone()
            } else {
                return plan_err!(
                    "cannot infer default catalog when multiple catalogs are defined"
                );
            }
        } else {
            return plan_err!("no catalogs are defined to infer default catalog");
        }
    };
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
        default_catalog,
        default_database: config.catalog.default_database.clone(),
        global_temporary_database: config.catalog.global_temporary_database.clone(),
    };
    CatalogManager::try_new(options)
        .map_err(|e| plan_datafusion_err!("failed to create catalog manager: {e}"))
}

fn wrap_catalog_provider(
    inner: Arc<dyn CatalogProvider>,
    name: &str,
    cache_config: &CatalogCacheConfig,
    cache_manager: &CatalogCacheManager,
) -> CatalogResult<Arc<dyn CatalogProvider>> {
    let has_global = matches!(cache_config.database_cache_type, CacheType::Global)
        || matches!(cache_config.table_cache_type, CacheType::Global)
        || matches!(cache_config.view_cache_type, CacheType::Global);

    let has_session = matches!(cache_config.database_cache_type, CacheType::Session)
        || matches!(cache_config.table_cache_type, CacheType::Session)
        || matches!(cache_config.view_cache_type, CacheType::Session);

    if !has_global && !has_session {
        return Ok(inner);
    }

    let global_bundle = if has_global {
        let mut bundle = cache_manager.get_cache(name)?;
        if bundle.is_none() {
            let new_bundle = Arc::new(sail_catalog::provider::CatalogCacheBundle::new(
                cache_config,
            ));
            cache_manager.set_cache(name.to_string(), new_bundle.clone())?;
            bundle = Some(new_bundle);
        }
        bundle
    } else {
        None
    };

    let provider = CachingCatalogProvider::new(inner, cache_config.clone(), global_bundle);
    Ok(Arc::new(provider))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use sail_common::config::{AppConfig, CatalogCacheConfig, CatalogType};

    use super::*;

    #[tokio::test]
    async fn test_create_catalog_manager_with_cache() {
        let mut config = AppConfig::load().unwrap();
        config.catalog.list = vec![CatalogType::HiveMetastore {
            name: "hms".to_string(),
            uris: vec!["localhost:9083".to_string()],
            thrift_transport: None,
            auth: None,
            kerberos_service_principal: None,
            min_sasl_qop: None,
            connect_timeout_secs: None,
            cache: CatalogCacheConfig {
                database_cache_type: CacheType::Session,
                ..Default::default()
            },
        }];
        config.catalog.default_catalog = Some("hms".to_string());
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        create_catalog_manager(&config, runtime, Arc::new(CatalogCacheManager::new()))
            .expect("catalog manager creation should succeed");
    }

    #[tokio::test]
    async fn test_shared_catalog_cache() {
        let mut config = AppConfig::load().unwrap();
        config.catalog.list = vec![CatalogType::Glue {
            name: "glue".to_string(),
            region: None,
            endpoint_url: None,
            cache: CatalogCacheConfig {
                database_cache_type: CacheType::Global,
                ..Default::default()
            },
        }];

        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle.clone());
        let cache_manager = Arc::new(CatalogCacheManager::new());

        // First session
        let _ = create_catalog_manager(&config, runtime.clone(), cache_manager.clone())
            .expect("first creation should succeed");
        let bundle1 = cache_manager.get_cache("glue").unwrap().unwrap();

        // Second session
        let _ = create_catalog_manager(&config, runtime.clone(), cache_manager.clone())
            .expect("second creation should succeed");
        let bundle2 = cache_manager.get_cache("glue").unwrap().unwrap();

        // Verify that both sessions reuse the exact same cache bundle rather than
        // creating independent bundles that merely both contain caches.
        assert!(
            Arc::ptr_eq(&bundle1, &bundle2),
            "catalog cache bundle should be shared across sessions"
        );
    }

    #[tokio::test]
    async fn test_session_catalog_cache() {
        let mut config = AppConfig::load().unwrap();
        config.catalog.list = vec![CatalogType::Glue {
            name: "glue".to_string(),
            region: None,
            endpoint_url: None,
            cache: CatalogCacheConfig {
                database_cache_type: CacheType::Session,
                ..Default::default()
            },
        }];

        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle.clone());
        let cache_manager = Arc::new(CatalogCacheManager::new());

        // Create manager
        let _ = create_catalog_manager(&config, runtime.clone(), cache_manager.clone())
            .expect("creation should succeed");

        // Verify that NOTHING was stored in the global manager
        let bundle = cache_manager.get_cache("glue").unwrap();
        assert!(
            bundle.is_none(),
            "session cache should not be stored in global manager"
        );
    }
}
