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

/// How long the session does not ask the catalog again for the storage credentials
/// of a table, after the catalog failed to issue them (e.g. a server or network error),
/// which may be resolved soon.
const FAILED_TTL: Duration = Duration::from_secs(15);

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
pub(crate) async fn install_table_storage_credentials<C: SessionExtensionAccessor + ?Sized>(
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
    let name = table.join(".");
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
    let initial = match get(StorageAccess::Read).await {
        Ok(Some(initial)) => initial,
        Ok(None) => {
            stores.set_not_issued(&url, &owner, NOT_ISSUED_TTL);
            return Ok(());
        }
        // The storage may be accessible with the credentials of the environment
        // when the catalog refuses or fails to issue credentials. If it is not, the
        // error of the storage has the answer of the catalog.
        Err(
            CatalogError::Unauthorized(message)
            | CatalogError::Forbidden(message)
            | CatalogError::CredentialUnavailable(message),
        ) => return not_issued(ctx, &stores, &url, &owner, &name, &message, NOT_ISSUED_TTL),
        Err(CatalogError::External(message)) => {
            return not_issued(ctx, &stores, &url, &owner, &name, &message, FAILED_TTL);
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

/// Keeps accessing the storage location of the table with the credentials of the
/// environment for the given time, after the catalog refused or failed to issue storage
/// credentials for the table with the given message. The message is logged once, and
/// is added to the errors of the storage that missing access can cause.
fn not_issued<C: SessionExtensionAccessor + ?Sized>(
    ctx: &C,
    stores: &VendedObjectStores,
    url: &Url,
    owner: &str,
    name: &str,
    message: &str,
    duration: Duration,
) -> CatalogResult<()> {
    stores.report_refused(owner, name, message);
    stores.set_not_issued(url, owner, duration);
    let reason = format!(
        "the catalog did not issue storage credentials for table {name}, \
         so the storage credentials of the environment were used: {message}"
    );
    stores
        .install_not_issued(
            ctx.runtime_env().object_store_registry.as_ref(),
            url,
            owner,
            &reason,
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

    use datafusion::execution::context::SessionConfig;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, LakehouseAuthority, LakehouseExecutionContext,
        LakehouseOperation, LakehouseStatusHints, MetadataPointerAuthority, ScanAuthority,
        TableLifecycle,
    };
    use sail_object_store::VendedStoreFactory;

    use super::*;
    use crate::lakehouse::ResolveLakehouseTableRequest;
    use crate::testing::{ManagedDeltaCatalog, issued_storage_credentials, single_catalog_manager};

    const LOCATION: &str = "memory:///warehouse/items";

    struct TestSession {
        ctx: SessionContext,
        provider: Arc<ManagedDeltaCatalog>,
        /// The store that is used with the credentials issued by the catalog.
        vended: Arc<InMemory>,
        fetch: Arc<Mutex<Option<StorageCredentialFetch>>>,
    }

    impl TestSession {
        fn new(
            response: fn() -> CatalogResult<Option<TableStorageCredentials>>,
            stores: bool,
        ) -> Self {
            let provider = Arc::new(ManagedDeltaCatalog::with_credentials(LOCATION, response));
            let manager = single_catalog_manager(provider.clone()).unwrap();
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
            if stores {
                config = config.with_extension(Arc::new(VendedObjectStores::with_factory(factory)));
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
            let requests = self.provider.credential_requests.lock().unwrap();
            for (name, request) in requests.iter() {
                assert_eq!(name, "default.items");
                assert_eq!(request.location, LOCATION);
            }
            requests.iter().map(|(_, request)| request.access).collect()
        }
    }

    #[tokio::test]
    async fn storage_credentials_are_installed_once_for_catalog_managed_delta_tables() {
        let session = TestSession::new(issued_storage_credentials, true);
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
        let expected = issued_storage_credentials()
            .unwrap()
            .map(|x| TableStorageCredentials {
                access: StorageAccess::ReadWrite,
                ..x
            });
        assert_eq!(fetched.ok(), expected);
        // The credential for reading that the session has is not fetched again.
        let fetched = fetch(StorageAccess::Read).await;
        assert_eq!(fetched.ok(), issued_storage_credentials().unwrap());
        assert_eq!(
            session.requests(),
            vec![StorageAccess::Read, StorageAccess::ReadWrite]
        );
    }

    #[tokio::test]
    async fn storage_credentials_are_not_requested_for_other_tables_and_sessions() {
        // The commits of the table are not coordinated by the catalog.
        let session = TestSession::new(issued_storage_credentials, true);
        session.install(CommitAuthority::Filesystem).await.unwrap();
        // The session does not use storage credentials issued by catalogs.
        let plain = TestSession::new(issued_storage_credentials, false);
        plain
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        for session in [session, plain] {
            assert!(session.requests().is_empty());
            assert!(!session.is_vended().await);
        }
    }

    #[tokio::test]
    async fn storage_credentials_not_issued_and_refused() {
        let session = TestSession::new(|| Ok(None), true);
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
            || Err(CatalogError::CredentialUnavailable("HTTP 400".to_string())),
        ] {
            let session = TestSession::new(response, true);
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
            true,
        );
        session
            .install(CommitAuthority::DeltaRatifiedCommit)
            .await
            .unwrap();
        assert!(!session.is_vended().await);

        // Other errors are reported.
        let session = TestSession::new(|| Err(CatalogError::Internal("bug".to_string())), true);
        let result = session.install(CommitAuthority::DeltaRatifiedCommit).await;
        assert!(
            matches!(result, Err(CatalogError::Internal(_))),
            "{result:?}"
        );
    }

    /// When the catalog does not issue credentials and the storage cannot be accessed
    /// with the credentials of the environment either, the error of the storage has the
    /// answer of the catalog.
    #[tokio::test(start_paused = true)]
    async fn storage_errors_have_the_answer_of_the_catalog_that_issued_no_credentials() {
        const ANSWER: &str = "HTTP 400 Bad Request: the client is not allowed";
        let url = Url::parse(LOCATION).unwrap();
        let store = |session: &TestSession| {
            let registry = &session.ctx.runtime_env().object_store_registry;
            registry.get_store(&url).unwrap()
        };
        let path = Path::from("warehouse/items/file");
        let other = Path::from("warehouse/other/file");
        // The in-memory store reports a range that is out of bounds as a generic error,
        // which is the kind of error of a store that has no access.
        let failure = |store: Arc<dyn ObjectStore>, path: Path| async move {
            store.put(&path, "data".into()).await.unwrap();
            store.get_range(&path, 100..200).await.unwrap_err()
        };
        for (response, ttl) in [
            (
                (|| Err(CatalogError::CredentialUnavailable(ANSWER.to_string()))) as fn() -> _,
                NOT_ISSUED_TTL,
            ),
            (
                || Err(CatalogError::Forbidden(ANSWER.to_string())),
                NOT_ISSUED_TTL,
            ),
            (
                || Err(CatalogError::External(ANSWER.to_string())),
                FAILED_TTL,
            ),
        ] {
            let session = TestSession::new(response, true);
            session
                .install(CommitAuthority::DeltaRatifiedCommit)
                .await
                .unwrap();
            assert!(!session.is_vended().await);

            let error = failure(store(&session), path.clone()).await;
            assert!(
                matches!(error, object_store::Error::Generic { .. }),
                "{error:?}"
            );
            let message = error.to_string();
            assert!(
                message.contains(ANSWER) && message.contains("default.items"),
                "{message}"
            );
            // An error that missing access does not cause, and the errors of other
            // locations, are as before.
            let error = store(&session)
                .get(&Path::from("warehouse/items/missing"))
                .await
                .unwrap_err();
            assert!(
                matches!(error, object_store::Error::NotFound { .. }),
                "{error:?}"
            );
            assert!(!error.to_string().contains(ANSWER), "{error}");
            let error = failure(store(&session), other.clone()).await;
            assert!(!error.to_string().contains(ANSWER), "{error}");

            // The catalog is asked again when the table is next used, since the storage
            // could not be accessed with the credentials of the environment.
            let _ = ttl;
            session
                .install(CommitAuthority::DeltaRatifiedCommit)
                .await
                .unwrap();
            assert_eq!(
                session.requests(),
                vec![StorageAccess::Read, StorageAccess::Read]
            );
            // It is not asked again while the storage is accessed without such an error.
            session
                .install(CommitAuthority::DeltaRatifiedCommit)
                .await
                .unwrap();
            assert_eq!(
                session.requests(),
                vec![StorageAccess::Read, StorageAccess::Read]
            );
        }

        // The credentials are used once the catalog issues them.
        thread_local! {
            static REFUSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }
        let session = TestSession::new(
            || {
                if REFUSED.replace(true) {
                    issued_storage_credentials()
                } else {
                    Err(CatalogError::CredentialUnavailable(ANSWER.to_string()))
                }
            },
            true,
        );
        let install = || session.install(CommitAuthority::DeltaRatifiedCommit);
        install().await.unwrap();
        assert!(!session.is_vended().await);
        tokio::time::advance(NOT_ISSUED_TTL + Duration::from_secs(1)).await;
        install().await.unwrap();
        assert!(session.is_vended().await);
        let error = store(&session)
            .get(&Path::from("warehouse/items/missing"))
            .await
            .unwrap_err();
        assert!(!error.to_string().contains(ANSWER), "{error}");
    }

    #[tokio::test]
    async fn storage_credentials_not_issued_are_not_requested_again_for_a_while() {
        for response in [
            (|| Ok(None)) as fn() -> _,
            || Err(CatalogError::Forbidden("no privilege".to_string())),
            || Err(CatalogError::External("server error".to_string())),
        ] {
            let session = TestSession::new(response, true);
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

    #[tokio::test(start_paused = true)]
    async fn storage_credentials_are_requested_again_sooner_after_a_failure() {
        for (response, ttl) in [
            ((|| Ok(None)) as fn() -> _, NOT_ISSUED_TTL),
            (
                || Err(CatalogError::Forbidden("no privilege".to_string())),
                NOT_ISSUED_TTL,
            ),
            (
                || Err(CatalogError::Unauthorized("no token".to_string())),
                NOT_ISSUED_TTL,
            ),
            (
                || Err(CatalogError::CredentialUnavailable("HTTP 400".to_string())),
                NOT_ISSUED_TTL,
            ),
            (
                || Err(CatalogError::External("server error".to_string())),
                FAILED_TTL,
            ),
        ] {
            let session = TestSession::new(response, true);
            let install = || session.install(CommitAuthority::DeltaRatifiedCommit);
            install().await.unwrap();
            tokio::time::advance(ttl - Duration::from_secs(1)).await;
            install().await.unwrap();
            assert_eq!(session.requests(), vec![StorageAccess::Read], "{ttl:?}");
            tokio::time::advance(Duration::from_secs(2)).await;
            install().await.unwrap();
            assert_eq!(
                session.requests(),
                vec![StorageAccess::Read, StorageAccess::Read],
                "{ttl:?}"
            );
        }
    }

    #[tokio::test]
    async fn storage_credentials_are_fetched_for_the_table_resolved_at_installation() {
        let session = TestSession::new(issued_storage_credentials, true);
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
        // All requests are for `default.items`, which is checked here.
        assert_eq!(
            session.requests(),
            vec![StorageAccess::Read, StorageAccess::ReadWrite]
        );
    }

    #[tokio::test]
    async fn storage_credentials_are_removed_with_the_table() {
        let session = TestSession::new(issued_storage_credentials, true);
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

    #[tokio::test]
    async fn storage_credentials_are_installed_when_the_catalog_resolves_the_table() {
        let table = ["items".to_string()];
        let resolve = TestSession::new(issued_storage_credentials, true);
        let manager = resolve.ctx.extension::<CatalogManager>().unwrap();
        let request = ResolveLakehouseTableRequest {
            catalog_table: table.to_vec(),
            operation: LakehouseOperation::Maintenance,
            requested_format: None,
            options: vec![],
        };
        manager
            .resolve_lakehouse_table(&resolve.ctx, &table, request)
            .await
            .unwrap();

        let resolve_status = TestSession::new(issued_storage_credentials, true);
        let manager = resolve_status.ctx.extension::<CatalogManager>().unwrap();
        let status = manager.get_table(&table).await.unwrap();
        manager
            .resolve_lakehouse_table_status(
                &resolve_status.ctx,
                &table,
                &status,
                LakehouseOperation::Alter,
            )
            .await
            .unwrap();

        for session in [resolve, resolve_status] {
            assert!(session.is_vended().await);
            assert_eq!(session.requests(), vec![StorageAccess::Read]);
        }
    }
}
