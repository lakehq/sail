//! Exercise the procedure's pointer-publication boundary through CatalogManager.
#![expect(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::*;
use sail_common_datafusion::catalog::managed::{
    METADATA_LOCATION_UNDERSCORE_KEY, PREVIOUS_METADATA_LOCATION_KEY,
};
use sail_common_datafusion::catalog::*;

use super::*;
use crate::io::test_store::{PutFault, TestStore};
use crate::procedure::rewrite_manifests::tests::{operation, output_schema};

#[derive(Clone, Copy)]
enum CasFault {
    None,
    Conflicts(usize),
    LostAck,
    Pause,
}

struct PointerCatalog {
    pointer: Mutex<String>,
    attempts: AtomicUsize,
    fault: CasFault,
    paused: tokio::sync::Notify,
}

fn unsupported<T>() -> CatalogResult<T> {
    Err(CatalogError::NotSupported(
        "unused test catalog operation".into(),
    ))
}

#[async_trait::async_trait]
impl CatalogProvider for PointerCatalog {
    fn get_name(&self) -> &str {
        "test"
    }
    async fn get_table(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        Ok(TableStatus {
            catalog: Some("test".into()),
            database: vec!["default".into()],
            name: "items".into(),
            kind: TableKind::Table {
                columns: vec![],
                comment: None,
                constraints: vec![],
                location: None,
                format: "iceberg".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                properties: vec![(
                    METADATA_LOCATION_UNDERSCORE_KEY.into(),
                    self.pointer.lock().expect("pointer").clone(),
                )],
                is_external: true,
            },
        })
    }
    async fn alter_table(
        &self,
        _: &Namespace,
        _: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let AlterTableOptions::SetTableProperties { properties } = options else {
            return unsupported();
        };
        let properties = properties.into_iter().collect::<HashMap<_, _>>();
        if matches!(self.fault, CasFault::Conflicts(count) if attempt <= count) {
            return Err(CatalogError::Conflict("injected pointer conflict".into()));
        }
        if matches!(self.fault, CasFault::Pause) {
            self.paused.notify_one();
            std::future::pending::<()>().await;
        }
        let mut pointer = self.pointer.lock().expect("pointer");
        assert_eq!(&properties[PREVIOUS_METADATA_LOCATION_KEY], &*pointer);
        *pointer = properties[METADATA_LOCATION_UNDERSCORE_KEY].clone();
        if matches!(self.fault, CasFault::LostAck) {
            Err(CatalogError::CommitStateUnknown(
                "injected lost pointer acknowledgement".to_string().into(),
            ))
        } else {
            Ok(())
        }
    }
    async fn create_database(
        &self,
        _: &Namespace,
        _: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        unsupported()
    }
    async fn get_database(&self, _: &Namespace) -> CatalogResult<DatabaseStatus> {
        unsupported()
    }
    async fn list_databases(&self, _: Option<&Namespace>) -> CatalogResult<Vec<DatabaseStatus>> {
        unsupported()
    }
    async fn drop_database(&self, _: &Namespace, _: DropDatabaseOptions) -> CatalogResult<()> {
        unsupported()
    }
    async fn create_table(
        &self,
        _: &Namespace,
        _: &str,
        _: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        unsupported()
    }
    async fn list_tables(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        unsupported()
    }
    async fn drop_table(&self, _: &Namespace, _: &str, _: DropTableOptions) -> CatalogResult<()> {
        unsupported()
    }
    async fn create_view(
        &self,
        _: &Namespace,
        _: &str,
        _: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        unsupported()
    }
    async fn get_view(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        unsupported()
    }
    async fn list_views(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        unsupported()
    }
    async fn drop_view(&self, _: &Namespace, _: &str, _: DropViewOptions) -> CatalogResult<()> {
        unsupported()
    }
}

async fn fixture(
    cas_fault: CasFault,
    put_fault: Option<PutFault>,
) -> (
    Arc<TestStore>,
    Arc<PointerCatalog>,
    Arc<TaskContext>,
    url::Url,
    LakehouseExecutionContext,
) {
    let (store, _, url, _) = super::tests::commit_fixture(None, false, false).await;
    let mut store = TestStore::new(store.inner.clone());
    store.fault = put_fault.map(|fault| ("00002-".into(), fault));
    let store = Arc::new(store);
    let catalog = Arc::new(PointerCatalog {
        pointer: Mutex::new(
            url.join("metadata/v1.metadata.json")
                .expect("metadata URL")
                .to_string(),
        ),
        attempts: AtomicUsize::new(0),
        fault: cas_fault,
        paused: tokio::sync::Notify::new(),
    });
    let manager = CatalogManager::try_new(CatalogManagerOptions {
        catalogs: HashMap::from([("test".into(), catalog.clone() as Arc<dyn CatalogProvider>)]),
        default_catalog: "test".into(),
        default_database: vec!["default".into()],
        global_temporary_database: vec!["global_temp".into()],
    })
    .expect("manager");
    let session =
        SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(manager)));
    session
        .runtime_env()
        .register_object_store(&url, store.clone());
    let mut context = LakehouseExecutionContext::catalog_table_context(
        CatalogProviderId("test".into()),
        vec!["test".into(), "default".into(), "items".into()],
        CatalogTableIdentity {
            table_id: None,
            table_uri: Some(url.to_string()),
        },
        LakehouseOperation::Maintenance,
        LakehouseFormat::Iceberg,
        LakehouseAuthority::PathManaged,
        ScanAuthority::ClientLakeSource,
    );
    context.commit = CommitAuthority::IcebergMetadataLocationCas;
    (store, catalog, session.task_ctx(), url, context)
}

async fn assert_pointer_is_readable(store: &TestStore, catalog: &PointerCatalog, url: &url::Url) {
    let pointer = catalog.pointer.lock().expect("pointer").clone();
    let store = StoreContext::new(store.inner.clone(), url).expect("store");
    let (base, path) = store.resolve(&pointer).expect("path");
    let metadata = TableMetadata::from_json(
        &base
            .get(&path)
            .await
            .expect("metadata")
            .bytes()
            .await
            .expect("bytes"),
    )
    .expect("metadata");
    let files = crate::table::files::collect_live_files(&store, &metadata, 1, |file| {
        Ok(Some(file.file_path))
    })
    .await
    .expect("read files");
    assert_eq!(files.len(), 2);
}

#[tokio::test]
async fn unique_metadata_and_manifests_follow_definite_cas_outcomes() {
    for conflicts in [0, 1, MAX_PROCEDURE_COMMIT_RETRIES] {
        let (store, catalog, ctx, url, context) =
            fixture(CasFault::Conflicts(conflicts), None).await;
        let before = store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        let result = commit_snapshot_operation(
            &ctx,
            &url,
            &[],
            Some(&context),
            operation(),
            output_schema(),
        )
        .await;
        assert_eq!(result.is_err(), conflicts == MAX_PROCEDURE_COMMIT_RETRIES);
        assert_eq!(
            catalog.attempts.load(Ordering::SeqCst),
            (conflicts + 1).min(MAX_PROCEDURE_COMMIT_RETRIES)
        );
        assert_eq!(store.deletes.lock().expect("deletes").len(), conflicts * 3);
        if conflicts == MAX_PROCEDURE_COMMIT_RETRIES {
            assert_eq!(
                store
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("list"),
                before
            );
        } else {
            assert_pointer_is_readable(&store, &catalog, &url).await;
        }
    }
}

#[tokio::test]
async fn unknown_cas_outcome_preserves_published_metadata_and_manifests() {
    let (store, catalog, ctx, url, context) = fixture(CasFault::LostAck, None).await;
    let error = commit_snapshot_operation(
        &ctx,
        &url,
        &[],
        Some(&context),
        operation(),
        output_schema(),
    )
    .await
    .expect_err("lost ack");
    assert!(
        error
            .to_string()
            .contains("injected lost pointer acknowledgement")
    );
    assert!(store.deletes.lock().expect("deletes").is_empty());
    assert_eq!(catalog.attempts.load(Ordering::SeqCst), 1);
    assert_pointer_is_readable(&store, &catalog, &url).await;
}

#[tokio::test]
async fn unique_metadata_upload_failures_never_reach_pointer_publication() {
    for fault in [PutFault::Reject, PutFault::Collision, PutFault::LostAck] {
        let (store, catalog, ctx, url, context) = fixture(CasFault::None, Some(fault)).await;
        let before = store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .len();
        assert!(
            commit_snapshot_operation(
                &ctx,
                &url,
                &[],
                Some(&context),
                operation(),
                output_schema()
            )
            .await
            .is_err()
        );
        assert_eq!(catalog.attempts.load(Ordering::SeqCst), 0);
        let attempts = if matches!(fault, PutFault::Collision) {
            MAX_PROCEDURE_COMMIT_RETRIES
        } else {
            1
        };
        let deletes = store.deletes.lock().expect("deletes").clone();
        assert_eq!(deletes.len(), attempts * 2);
        assert!(
            deletes
                .iter()
                .all(|path| !path.as_ref().ends_with(".metadata.json"))
        );
        // A failed Create is never owned, even if its acknowledgement was lost.
        let remaining = store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        assert_eq!(
            remaining.len(),
            before + usize::from(matches!(fault, PutFault::LostAck))
        );
        assert_pointer_is_readable(&store, &catalog, &url).await;
    }
}

#[tokio::test]
async fn cancellation_before_and_during_cas_respects_publication_boundary() {
    for during_cas in [false, true] {
        let (store, catalog, ctx, url, context) = fixture(
            if during_cas {
                CasFault::Pause
            } else {
                CasFault::None
            },
            (!during_cas).then_some(PutFault::Pause),
        )
        .await;
        let before = store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .len();
        let task = tokio::spawn(async move {
            commit_snapshot_operation(
                &ctx,
                &url,
                &[],
                Some(&context),
                operation(),
                output_schema(),
            )
            .await
        });
        if during_cas {
            catalog.paused.notified().await;
        } else {
            store.paused.notified().await;
        }
        task.abort();
        assert!(task.await.expect_err("canceled").is_cancelled());
        if during_cas {
            assert!(store.deletes.lock().expect("deletes").is_empty());
            assert_eq!(
                store
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("list")
                    .len(),
                before + 3
            );
        } else {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if store
                        .inner
                        .list(None)
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("list")
                        .len()
                        == before
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cleanup completed");
            assert_eq!(store.deletes.lock().expect("deletes").len(), 2);
        }
    }
}
