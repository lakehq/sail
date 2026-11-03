use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Result, plan_datafusion_err};
use datafusion_common::plan_err;
use sail_catalog::credentials::{
    CatalogCredentials, EmptyCatalogCredentials, FileCatalogCredentials, StaticCatalogCredentials,
};
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::{
    CachingCatalogProvider, CatalogCacheManager, CatalogProvider, RuntimeAwareCatalogProvider,
};
use sail_catalog_glue::{GlueCatalogConfig, GlueCatalogProvider};
use sail_catalog_hms::{HmsCatalogConfig, HmsCatalogProvider};
use sail_catalog_iceberg::{IcebergRestCatalogOptions, IcebergRestCatalogProvider};
use sail_catalog_memory::MemoryCatalogProvider;
use sail_catalog_onelake::{OneLakeApiKind, OneLakeCatalogProvider};
use sail_catalog_system::{SYSTEM_CATALOG_NAME, SystemCatalogProvider};
use sail_catalog_unity::{UnityCatalogConfig, UnityCatalogOptions, UnityCatalogProvider};
use sail_common::config::{AppConfig, CacheType, CatalogCacheConfig, CatalogType, OneLakeApi};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::extension::SessionExtension;
use secrecy::{ExposeSecret, SecretString};

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
                    namespace_separator,
                    oauth_access_token,
                    bearer_access_token,
                    bearer_access_token_file,
                    cache,
                } => {
                    let credentials = iceberg_rest_credentials(
                        bearer_access_token_file.as_ref(),
                        bearer_access_token.as_ref(),
                        oauth_access_token.as_ref(),
                    );
                    let runtime_aware = iceberg_rest_provider(
                        name,
                        uri,
                        warehouse.as_deref(),
                        prefix.as_deref(),
                        namespace_separator.as_deref(),
                        credentials,
                        &runtime,
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
                        || {
                            let config = UnityCatalogConfig::new(uri.clone(), token, None)?;
                            let credentials = config
                                .get_credential_provider()
                                .map(|credentials| {
                                    Arc::new(credentials) as Arc<dyn CatalogCredentials>
                                })
                                .unwrap_or_else(|| Arc::new(EmptyCatalogCredentials));
                            let default_catalog = default_catalog
                                .clone()
                                .unwrap_or_else(|| "unity".to_string());
                            UnityCatalogProvider::new(
                                name.to_string(),
                                UnityCatalogOptions {
                                    default_catalog,
                                    uri: config.uri,
                                    credentials,
                                    quote_object_name: true,
                                },
                            )
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
                CatalogType::OneLake {
                    name,
                    url,
                    api,
                    bearer_token,
                    cache,
                } => {
                    // Parse URL format:
                    //   - `workspace/item.type`: friendly names (e.g. "duckrun/data.lakehouse")
                    //   - `workspaceId/itemId`: GUIDs (e.g. "8f.../3a...").
                    // GUID form is required when the workspace has friendly-name support disabled,
                    // because the data/metadata paths are then GUID-addressed at the storage layer.
                    let (workspace, item) = url.split_once('/').ok_or_else(|| {
                        plan_datafusion_err!(
                            "Invalid OneLake URL format: expected 'workspace/item.type' or 'workspaceId/itemId', got '{}'",
                            url
                        )
                    })?;

                    // Friendly form: ("data.Lakehouse" -> name="data", type=Some("Lakehouse"))
                    // GUID form: ("data" -> name="data", type=None)
                    let (item_name, item_type) = match item.split_once('.') {
                        Some((name, item_type)) => (name, Some(item_type.to_string())),
                        None => (item, None),
                    };

                    let token = bearer_token.as_ref().map(|t| t.expose_secret().to_string());
                    let api = match api {
                        OneLakeApi::Delta => OneLakeApiKind::Delta,
                        OneLakeApi::Iceberg => OneLakeApiKind::Iceberg,
                    };
                    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
                        || {
                            OneLakeCatalogProvider::new(
                                name.clone(),
                                workspace.to_string(),
                                item_name.to_string(),
                                item_type,
                                api,
                                token.clone(),
                            )
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
                    catalog_id,
                    region,
                    endpoint_url,
                    cache,
                } => {
                    let config = GlueCatalogConfig {
                        catalog_id: catalog_id.clone(),
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

/// The value of `spark.sql.catalog.<name>` that registers an Iceberg REST catalog.
pub const SESSION_CATALOG_ICEBERG_REST: &str = "iceberg-rest";

/// Builds the catalogs that a session registers at runtime, for example from the
/// `spark.sql.catalog.<name>*` options of a Spark session.
///
/// A session catalog is registered in the session's [`CatalogManager`] only, so it is
/// invisible to other sessions and is dropped with the session. It has no catalog cache,
/// since a global cache is keyed by the catalog name and so would be shared across sessions.
pub struct SessionCatalogFactory {
    runtime: RuntimeHandle,
}

impl SessionExtension for SessionCatalogFactory {
    fn name() -> &'static str {
        "session catalog factory"
    }
}

impl SessionCatalogFactory {
    pub fn new(runtime: RuntimeHandle) -> Self {
        Self { runtime }
    }

    /// Makes the catalog `name` match its definition: the value of `spark.sql.catalog.<name>`
    /// and the options under `spark.sql.catalog.<name>.`, without that prefix.
    ///
    /// The catalog is registered once its type is known and, for an Iceberg REST catalog,
    /// its `uri` is set. Until then, and when the type is not supported, the catalog is removed.
    pub fn apply(
        &self,
        manager: &CatalogManager,
        name: &str,
        kind: Option<&str>,
        options: &HashMap<String, String>,
    ) -> CatalogResult<()> {
        let provider = match kind {
            Some(SESSION_CATALOG_ICEBERG_REST) => {
                if manager.has_fixed_catalog(name)? {
                    return Err(CatalogError::AlreadyExists(
                        CatalogObject::Catalog,
                        name.to_string(),
                    ));
                }
                self.iceberg_rest(name, options)?
            }
            _ => None,
        };
        match provider {
            Some(provider) => manager.register_session_catalog(name, provider),
            None => manager.remove_session_catalog(name),
        }
    }

    fn iceberg_rest(
        &self,
        name: &str,
        options: &HashMap<String, String>,
    ) -> CatalogResult<Option<Arc<dyn CatalogProvider>>> {
        if options.contains_key("bearer_access_token_file") {
            // The server would read the file and send its content to the catalog `uri`.
            return Err(CatalogError::NotSupported(
                "bearer_access_token_file for a session catalog".to_string(),
            ));
        }
        let Some(uri) = options.get("uri") else {
            return Ok(None);
        };
        let secret = |key: &str| options.get(key).map(|x| SecretString::from(x.clone()));
        let credentials = iceberg_rest_credentials(
            None,
            secret("bearer_access_token").as_ref(),
            secret("oauth_access_token").as_ref(),
        );
        let provider = iceberg_rest_provider(
            name,
            uri,
            options.get("warehouse").map(String::as_str),
            options.get("prefix").map(String::as_str),
            options.get("namespace_separator").map(String::as_str),
            credentials,
            &self.runtime,
        )?;
        Ok(Some(Arc::new(provider)))
    }
}

fn iceberg_rest_provider(
    name: &str,
    uri: &str,
    warehouse: Option<&str>,
    prefix: Option<&str>,
    namespace_separator: Option<&str>,
    credentials: Arc<dyn CatalogCredentials>,
    runtime: &RuntimeHandle,
) -> CatalogResult<RuntimeAwareCatalogProvider<IcebergRestCatalogProvider>> {
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
    RuntimeAwareCatalogProvider::try_new(
        || {
            Ok(IcebergRestCatalogProvider::new(
                name.to_string(),
                IcebergRestCatalogOptions {
                    credentials,
                    properties,
                },
            ))
        },
        runtime.io().clone(),
    )
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

/// Credentials for an Iceberg REST catalog. A token file takes precedence over
/// a static token: its contents are re-read per request, so a rotated projected
/// service account token is picked up without a restart.
fn iceberg_rest_credentials(
    bearer_access_token_file: Option<&String>,
    bearer_access_token: Option<&SecretString>,
    oauth_access_token: Option<&SecretString>,
) -> Arc<dyn CatalogCredentials> {
    if let Some(path) = bearer_access_token_file {
        Arc::new(FileCatalogCredentials::new(path)) as Arc<dyn CatalogCredentials>
    } else {
        bearer_access_token
            .or(oauth_access_token)
            .map(|token| {
                Arc::new(StaticCatalogCredentials::new(
                    token.expose_secret().to_string(),
                )) as Arc<dyn CatalogCredentials>
            })
            .unwrap_or_else(|| Arc::new(EmptyCatalogCredentials))
    }
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
            catalog_id: None,
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
            catalog_id: None,
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

    #[tokio::test]
    async fn iceberg_credentials_prefer_the_token_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "file-token\n").unwrap();
        let path = path.to_string_lossy().to_string();
        let static_token = SecretString::from("static-token".to_string());

        let credentials = iceberg_rest_credentials(Some(&path), Some(&static_token), None);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("file-token".to_string())
        );
    }

    #[tokio::test]
    async fn iceberg_credentials_use_the_static_bearer_token() {
        let token = SecretString::from("static-token".to_string());
        let credentials = iceberg_rest_credentials(None, Some(&token), None);
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("static-token".to_string())
        );
    }

    #[tokio::test]
    async fn iceberg_credentials_fall_back_to_the_oauth_token() {
        let token = SecretString::from("oauth-token".to_string());
        let credentials = iceberg_rest_credentials(None, None, Some(&token));
        assert_eq!(
            credentials.retrieve().await.unwrap(),
            Some("oauth-token".to_string())
        );
    }

    #[tokio::test]
    async fn iceberg_credentials_are_empty_without_any_token() {
        let credentials = iceberg_rest_credentials(None, None, None);
        assert_eq!(credentials.retrieve().await.unwrap(), None);
    }

    fn session_catalog_manager() -> (CatalogManager, SessionCatalogFactory) {
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let mut config = AppConfig::load().unwrap();
        config.catalog.list = vec![CatalogType::Memory {
            name: "sail".to_string(),
            initial_database: vec!["default".to_string()],
            initial_database_comment: None,
        }];
        let manager = create_catalog_manager(
            &config,
            runtime.clone(),
            Arc::new(CatalogCacheManager::new()),
        )
        .unwrap();
        (manager, SessionCatalogFactory::new(runtime))
    }

    fn iceberg_options(token: &str) -> HashMap<String, String> {
        HashMap::from([
            (
                "uri".to_string(),
                "http://catalog.invalid/catalog".to_string(),
            ),
            ("warehouse".to_string(), "org_1".to_string()),
            ("bearer_access_token".to_string(), token.to_string()),
        ])
    }

    #[tokio::test]
    async fn session_catalog_registers_and_resolves() {
        let (manager, factory) = session_catalog_manager();
        factory
            .apply(
                &manager,
                "org_1",
                Some(SESSION_CATALOG_ICEBERG_REST),
                &iceberg_options("token"),
            )
            .unwrap();
        let mut names = manager.list_catalogs(None).unwrap();
        names.sort();
        assert_eq!(
            names.iter().map(|x| x.as_ref()).collect::<Vec<_>>(),
            vec!["org_1", "sail", "system"]
        );
        assert!(manager.resolve_catalog_reference(Some("org_1")).is_ok());
    }

    #[tokio::test]
    async fn session_catalogs_of_two_managers_are_separate() {
        let (first, factory) = session_catalog_manager();
        let (second, _) = session_catalog_manager();
        factory
            .apply(
                &first,
                "org_1",
                Some(SESSION_CATALOG_ICEBERG_REST),
                &iceberg_options("token"),
            )
            .unwrap();
        assert!(first.resolve_catalog_reference(Some("org_1")).is_ok());
        assert!(second.resolve_catalog_reference(Some("org_1")).is_err());
    }

    #[tokio::test]
    async fn session_catalog_cannot_replace_a_server_or_the_system_catalog() {
        let (manager, factory) = session_catalog_manager();
        for name in ["sail", SYSTEM_CATALOG_NAME] {
            let error = factory
                .apply(
                    &manager,
                    name,
                    Some(SESSION_CATALOG_ICEBERG_REST),
                    &iceberg_options("token"),
                )
                .unwrap_err();
            assert!(matches!(error, CatalogError::AlreadyExists(..)), "{error}");
        }
        // Removing a name that the session did not register keeps the server catalog.
        factory
            .apply(&manager, "sail", None, &HashMap::new())
            .unwrap();
        assert!(manager.resolve_catalog_reference(Some("sail")).is_ok());
    }

    #[tokio::test]
    async fn session_catalog_is_removed_without_a_type_or_a_uri() {
        let (manager, factory) = session_catalog_manager();
        let kind = Some(SESSION_CATALOG_ICEBERG_REST);
        factory
            .apply(&manager, "org_1", kind, &iceberg_options("token"))
            .unwrap();
        factory
            .apply(&manager, "org_1", kind, &HashMap::new())
            .unwrap();
        assert!(manager.resolve_catalog_reference(Some("org_1")).is_err());
        factory
            .apply(&manager, "org_1", kind, &iceberg_options("token"))
            .unwrap();
        factory
            .apply(&manager, "org_1", None, &iceberg_options("token"))
            .unwrap();
        assert!(manager.resolve_catalog_reference(Some("org_1")).is_err());
    }

    #[tokio::test]
    async fn session_catalog_refuses_a_token_file() {
        let (manager, factory) = session_catalog_manager();
        let mut options = iceberg_options("token");
        options.insert(
            "bearer_access_token_file".to_string(),
            "/var/run/secrets/token".to_string(),
        );
        let error = factory
            .apply(
                &manager,
                "org_1",
                Some(SESSION_CATALOG_ICEBERG_REST),
                &options,
            )
            .unwrap_err();
        assert!(matches!(error, CatalogError::NotSupported(_)), "{error}");
    }

    #[test]
    fn static_credentials_do_not_print_the_token() {
        let credentials = StaticCatalogCredentials::new("secret-token".to_string());
        assert!(!format!("{credentials:?}").contains("secret-token"));
    }
}
