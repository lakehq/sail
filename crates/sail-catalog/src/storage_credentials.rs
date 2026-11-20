// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use sail_common_datafusion::catalog::{
    CommitAuthority, LakehouseFormat, StorageAccess, TableStorageCredentials,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_object_store::{StorageCredentialFetch, VendedObjectStores};
use url::Url;

use crate::error::{CatalogError, CatalogResult};
use crate::lakehouse::{LakehouseResolvedTable, TableStorageCredentialsRequest};
use crate::manager::CatalogManager;

/// How long the session does not ask the catalog again for the storage credentials
/// of a table, after the catalog did not issue them.
const NOT_ISSUED_TTL: Duration = Duration::from_secs(300);

/// Returns the name that identifies the table in the session, regardless of the
/// current catalog and database.
fn table_owner(manager: &CatalogManager, table: &[String]) -> CatalogResult<(Vec<String>, String)> {
    let name = manager.resolve_fully_qualified_object_name(table)?;
    let owner = format!("{name:?}");
    Ok((name, owner))
}

/// Accesses the storage location of the table with the storage credentials that the
/// catalog issues for the table, for the rest of the session.
///
/// Nothing changes if the catalog does not issue storage credentials for the table,
/// or refuses to issue them, or fails to issue them, in which case the storage is
/// accessed with the credentials of the environment as before.
/// Only Delta tables whose commits are coordinated by the catalog are considered.
pub async fn install_table_storage_credentials<C: SessionExtensionAccessor>(
    ctx: &C,
    table: &[String],
    resolved: &LakehouseResolvedTable,
) -> CatalogResult<()> {
    if resolved.status.format != LakehouseFormat::Delta
        || resolved.execution.commit != CommitAuthority::DeltaRatifiedCommit
    {
        return Ok(());
    }
    let Some(location) = resolved.status.location.as_ref() else {
        return Ok(());
    };
    let Ok(url) = Url::parse(location) else {
        return Ok(());
    };
    let Ok(stores) = ctx.extension::<VendedObjectStores>() else {
        return Ok(());
    };
    let manager = ctx
        .extension::<CatalogManager>()
        .map_err(|e| CatalogError::Internal(e.to_string()))?;
    // The table is identified as it is resolved now, so that the credentials are
    // fetched for the same table later, even if the current database changes.
    let (table, owner) = table_owner(&manager, table)?;
    if stores.contains(&url, &owner) || stores.is_not_issued(&url, &owner) {
        return Ok(());
    }
    if !stores.is_enabled() {
        stores.report_disabled();
        return Ok(());
    }
    let get = {
        let table = table.clone();
        let location = location.clone();
        move |access: StorageAccess| {
            let manager = Arc::clone(&manager);
            let table = table.clone();
            let request = TableStorageCredentialsRequest {
                location: location.clone(),
                access,
            };
            async move { manager.get_table_storage_credentials(&table, request).await }
        }
    };
    let name = table.join(".");
    let initial = match get(StorageAccess::Read).await {
        Ok(Some(initial)) => initial,
        Ok(None) => {
            stores.set_not_issued(&url, &owner, NOT_ISSUED_TTL);
            return Ok(());
        }
        // The storage may be accessible with the credentials of the environment
        // when the catalog refuses or fails to issue credentials.
        Err(
            CatalogError::Unauthorized(message)
            | CatalogError::Forbidden(message)
            | CatalogError::External(message),
        ) => {
            stores.report_refused(&owner, &name, &message);
            stores.set_not_issued(&url, &owner, NOT_ISSUED_TTL);
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let fetch: StorageCredentialFetch = Arc::new(move |access| {
        let get = get(access);
        let name = name.clone();
        Box::pin(async move {
            match get.await {
                Ok(Some(credentials)) => Ok::<TableStorageCredentials, String>(credentials),
                Ok(None) => Err(format!(
                    "the catalog does not issue storage credentials for table {name}"
                )),
                Err(e) => Err(format!(
                    "failed to get storage credentials for table {name}: {e}"
                )),
            }
        })
    });
    stores
        .install(
            ctx.runtime_env().object_store_registry.as_ref(),
            &url,
            &owner,
            initial,
            fetch,
        )
        .map_err(|e| CatalogError::External(e.to_string()))
}

/// Stops accessing the storage location of the table with the storage credentials
/// that the catalog issued for the table, e.g. after the table is dropped.
pub fn remove_table_storage_credentials<C: SessionExtensionAccessor>(ctx: &C, table: &[String]) {
    let (Ok(stores), Ok(manager)) = (
        ctx.extension::<VendedObjectStores>(),
        ctx.extension::<CatalogManager>(),
    ) else {
        return;
    };
    if let Ok((_, owner)) = table_owner(&manager, table) {
        stores.remove(&owner);
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::execution::context::SessionConfig;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, DatabaseStatus, LakehouseAuthority,
        LakehouseExecutionContext, LakehouseOperation, LakehouseStatusHints,
        MetadataPointerAuthority, ScanAuthority, StorageCredential, TableLifecycle, TableStatus,
    };
    use sail_object_store::VendedStoreFactory;

    use super::*;
    use crate::manager::CatalogManagerOptions;
    use crate::provider::{
        AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
        CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };

    const LOCATION: &str = "memory:///warehouse/items";

    /// A catalog that only issues storage credentials.
    struct VendingProvider {
        response: fn() -> CatalogResult<Option<TableStorageCredentials>>,
        requests: Mutex<Vec<(String, TableStorageCredentialsRequest)>>,
    }

    #[async_trait]
    impl CatalogProvider for VendingProvider {
        fn get_name(&self) -> &str {
            "unity"
        }

        async fn get_table_storage_credentials(
            &self,
            database: &Namespace,
            table: &str,
            request: TableStorageCredentialsRequest,
        ) -> CatalogResult<Option<TableStorageCredentials>> {
            let name = format!(
                "{}.{table}",
                Vec::<String>::from(database.clone()).join(".")
            );
            self.requests.lock().unwrap().push((name, request));
            (self.response)()
        }

        async fn create_database(
            &self,
            _database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unreachable!()
        }

        async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "unity".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unreachable!()
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_table(
            &self,
            _database: &Namespace,
            _table: &str,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn create_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }
    }

    fn issued() -> CatalogResult<Option<TableStorageCredentials>> {
        Ok(Some(TableStorageCredentials {
            credential: StorageCredential::GcsOauth {
                token: "secret-token".to_string(),
            },
            expires_at_ms: None,
        }))
    }

    struct TestSession {
        ctx: SessionContext,
        provider: Arc<VendingProvider>,
        /// The store that is used with the credentials issued by the catalog.
        vended: Arc<InMemory>,
        fetch: Arc<Mutex<Option<StorageCredentialFetch>>>,
    }

    impl TestSession {
        fn new(
            response: fn() -> CatalogResult<Option<TableStorageCredentials>>,
            stores: Option<bool>,
        ) -> Self {
            let provider = Arc::new(VendingProvider {
                response,
                requests: Mutex::new(Vec::new()),
            });
            let manager = CatalogManager::try_new(CatalogManagerOptions {
                catalogs: std::iter::once((
                    "unity".to_string(),
                    provider.clone() as Arc<dyn CatalogProvider>,
                ))
                .collect(),
                default_catalog: "unity".to_string(),
                default_database: vec!["default".to_string()],
                global_temporary_database: vec!["global_temp".to_string()],
            })
            .unwrap();
            let vended = Arc::new(InMemory::new());
            let fetch = Arc::new(Mutex::new(None));
            let factory: VendedStoreFactory = {
                let (vended, fetch) = (vended.clone(), fetch.clone());
                Arc::new(move |request| {
                    *fetch.lock().unwrap() = Some(request.fetch);
                    Ok(vended.clone() as Arc<dyn ObjectStore>)
                })
            };
            let mut config = SessionConfig::new().with_extension(Arc::new(manager));
            match stores {
                Some(true) => {
                    config =
                        config.with_extension(Arc::new(VendedObjectStores::with_factory(factory)));
                }
                Some(false) => {
                    let handle = tokio::runtime::Handle::current();
                    let runtime = sail_common::runtime::RuntimeHandle::new(handle.clone(), handle);
                    config =
                        config.with_extension(Arc::new(VendedObjectStores::new(runtime, false)));
                }
                None => {}
            }
            let ctx = SessionContext::new_with_config(config);
            ctx.runtime_env()
                .register_object_store(&Url::parse(LOCATION).unwrap(), Arc::new(InMemory::new()));
            Self {
                ctx,
                provider,
                vended,
                fetch,
            }
        }

        async fn install(&self, commit: CommitAuthority) -> CatalogResult<()> {
            self.install_as(&["default", "items"], commit).await
        }

        /// Installs the storage credentials for the table as it is referenced.
        async fn install_as(
            &self,
            reference: &[&str],
            commit: CommitAuthority,
        ) -> CatalogResult<()> {
            let table = vec!["default".to_string(), "items".to_string()];
            let reference = reference.iter().map(|x| x.to_string()).collect::<Vec<_>>();
            let resolved = LakehouseResolvedTable {
                status: LakehouseStatusHints {
                    lifecycle: TableLifecycle::Managed,
                    format: LakehouseFormat::Delta,
                    location: Some(LOCATION.to_string()),
                    display_properties: vec![],
                    cross_format_summary: None,
                },
                execution: LakehouseExecutionContext::catalog_table_context(
                    CatalogProviderId("unity".to_string()),
                    table.clone(),
                    CatalogTableIdentity {
                        table_id: None,
                        table_uri: Some(LOCATION.to_string()),
                    },
                    LakehouseOperation::Read,
                    LakehouseFormat::Delta,
                    LakehouseAuthority::CatalogAuthoritative {
                        lifecycle: TableLifecycle::Managed,
                        pointer: MetadataPointerAuthority::StorageDiscovery,
                        commit,
                    },
                    ScanAuthority::ClientLakeSource,
                ),
                runtime: None,
            };
            install_table_storage_credentials(&self.ctx, &reference, &resolved).await
        }

        /// Whether the table location is accessed with the credentials issued by the catalog.
        async fn is_vended(&self) -> bool {
            let path = Path::from("warehouse/items/file");
            self.vended.put(&path, "data".into()).await.unwrap();
            let store = self
                .ctx
                .runtime_env()
                .object_store_registry
                .get_store(&Url::parse(LOCATION).unwrap())
                .unwrap();
            store.get(&path).await.is_ok()
        }

        fn requests(&self) -> Vec<StorageAccess> {
            let requests = self.provider.requests.lock().unwrap();
            for (name, request) in requests.iter() {
                assert_eq!(name, "default.items");
                assert_eq!(request.location, LOCATION);
            }
            requests.iter().map(|(_, request)| request.access).collect()
        }
    }

    #[tokio::test]
    async fn storage_credentials_are_installed_once_for_catalog_managed_delta_tables() {
        let session = TestSession::new(issued, Some(true));
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert!(session.is_vended().await);
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert_eq!(session.requests(), vec![StorageAccess::Read]);

        // The credentials are fetched from the catalog again for the requested access.
        let fetch = session.fetch.lock().unwrap().clone().unwrap();
        let fetched = fetch(StorageAccess::ReadWrite).await;
        assert_eq!(fetched.ok(), issued().unwrap());
        assert_eq!(
            session.requests(),
            vec![StorageAccess::Read, StorageAccess::ReadWrite]
        );
    }

    #[tokio::test]
    async fn storage_credentials_are_not_requested_for_other_tables_and_sessions() {
        // The commits of the table are not coordinated by the catalog.
        let session = TestSession::new(issued, Some(true));
        session.install(CommitAuthority::Filesystem).await.unwrap();
        // The session runs in cluster mode.
        let cluster = TestSession::new(issued, Some(false));
        cluster
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        // The session does not use storage credentials issued by catalogs.
        let plain = TestSession::new(issued, None);
        plain
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        for session in [session, cluster, plain] {
            assert!(session.requests().is_empty());
            assert!(!session.is_vended().await);
        }
    }

    #[tokio::test]
    async fn storage_credentials_not_issued_and_refused() {
        let session = TestSession::new(|| Ok(None), Some(true));
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert_eq!(session.requests(), vec![StorageAccess::Read]);
        assert!(!session.is_vended().await);

        // The table is accessed as before if the catalog refuses to issue credentials.
        for response in [
            (|| Err(CatalogError::Forbidden("no privilege".to_string()))) as fn() -> _,
            || Err(CatalogError::Unauthorized("no token".to_string())),
        ] {
            let session = TestSession::new(response, Some(true));
            session
                .install(CommitAuthority::DeltaRatifiedCommit)
                .await
                .unwrap();
            assert_eq!(session.requests(), vec![StorageAccess::Read]);
            assert!(!session.is_vended().await);
        }

        // The catalog fails to issue credentials.
        let session = TestSession::new(
            || Err(CatalogError::External("server error".to_string())),
            Some(true),
        );
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert!(!session.is_vended().await);

        // Other errors are reported.
        let session = TestSession::new(
            || Err(CatalogError::Internal("bug".to_string())),
            Some(true),
        );
        let result = session.install(CommitAuthority::DeltaRatifiedCommit).await;
        assert!(
            matches!(result, Err(CatalogError::Internal(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn storage_credentials_not_issued_are_not_requested_again_for_a_while() {
        for response in [
            (|| Ok(None)) as fn() -> _,
            || Err(CatalogError::Forbidden("no privilege".to_string())),
            || Err(CatalogError::External("server error".to_string())),
        ] {
            let session = TestSession::new(response, Some(true));
            for _ in 0..3 {
                session
                    .install(CommitAuthority::DeltaRatifiedCommit)
                    .await
                    .unwrap();
            }
            assert_eq!(session.requests(), vec![StorageAccess::Read]);
            assert!(!session.is_vended().await);
        }
    }

    #[tokio::test]
    async fn storage_credentials_are_fetched_for_the_table_resolved_at_installation() {
        let session = TestSession::new(issued, Some(true));
        session
            .install_as(&["items"], CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        // The same table referenced by another name is the same table.
        session
            .install_as(
                &["unity", "default", "items"],
                CommitAuthority::DeltaRatifiedCommit,
            )
            .await
            .unwrap();
        assert_eq!(session.requests(), vec![StorageAccess::Read]);

        // The current database changes before the credentials are fetched again.
        let manager = session.ctx.extension::<CatalogManager>().unwrap();
        manager.set_default_database(vec!["other"]).await.unwrap();
        let fetch = session.fetch.lock().unwrap().clone().unwrap();
        assert!(fetch(StorageAccess::ReadWrite).await.is_ok());
        assert!(fetch(StorageAccess::Read).await.is_ok());
        // All requests are for `default.items`, which is checked here.
        assert_eq!(
            session.requests(),
            vec![
                StorageAccess::Read,
                StorageAccess::ReadWrite,
                StorageAccess::Read
            ]
        );
    }

    #[tokio::test]
    async fn storage_credentials_are_removed_with_the_table() {
        let session = TestSession::new(issued, Some(true));
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert!(session.is_vended().await);
        remove_table_storage_credentials(&session.ctx, &["items".to_string()]);
        assert!(!session.is_vended().await);
        // The table is created again.
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert!(session.is_vended().await);
        assert_eq!(
            session.requests(),
            vec![StorageAccess::Read, StorageAccess::Read]
        );
    }
}
