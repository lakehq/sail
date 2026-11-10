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
    Reject,
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
                properties: {
                    let pointer = self.pointer.lock().expect("pointer").clone();
                    if pointer.is_empty() {
                        vec![]
                    } else {
                        vec![(METADATA_LOCATION_UNDERSCORE_KEY.into(), pointer)]
                    }
                },
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
        if matches!(self.fault, CasFault::Reject) {
            return Err(CatalogError::NotSupported(
                "injected pointer rejection".into(),
            ));
        }
        if matches!(self.fault, CasFault::Pause) {
            self.paused.notify_one();
            std::future::pending::<()>().await;
        }
        let mut pointer = self.pointer.lock().expect("pointer");
        assert_eq!(
            properties
                .get(PREVIOUS_METADATA_LOCATION_KEY)
                .map(String::as_str)
                .unwrap_or(""),
            pointer.as_str(),
        );
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
    let pointer = url
        .join("metadata/v1.metadata.json")
        .expect("metadata URL")
        .to_string();
    pointer_fixture(cas_fault, Arc::new(store), url, pointer)
}

fn pointer_fixture(
    cas_fault: CasFault,
    store: Arc<TestStore>,
    url: url::Url,
    pointer: String,
) -> (
    Arc<TestStore>,
    Arc<PointerCatalog>,
    Arc<TaskContext>,
    url::Url,
    LakehouseExecutionContext,
) {
    let catalog = Arc::new(PointerCatalog {
        pointer: Mutex::new(pointer),
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
            crate::io::test_store::assert_commit_retryability(
                &result.expect_err("conflict"),
                false,
            );
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
    crate::io::test_store::assert_commit_retryability(&error, true);
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
        let error = commit_snapshot_operation(
            &ctx,
            &url,
            &[],
            Some(&context),
            operation(),
            output_schema(),
        )
        .await
        .expect_err("upload failure");
        crate::io::test_store::assert_commit_retryability(&error, false);
        if !matches!(fault, PutFault::Collision) {
            assert!(
                matches!(&error, DataFusionError::External(source) if source.is::<object_store::Error>())
            );
        }
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
async fn filesystem_pointer_errors_after_create_are_terminal() {
    for fault in [CasFault::Reject, CasFault::Conflicts(1), CasFault::LostAck] {
        let (store, catalog, ctx, url, mut context) = fixture(fault, None).await;
        context.commit = CommitAuthority::Filesystem;
        let error = commit_snapshot_operation(
            &ctx,
            &url,
            &[],
            Some(&context),
            operation(),
            output_schema(),
        )
        .await
        .expect_err("pointer update error");
        crate::io::test_store::assert_commit_retryability(&error, true);
        assert_eq!(catalog.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(store.puts.load(Ordering::SeqCst), 3);
        assert!(store.deletes.lock().expect("deletes").is_empty());
        let reader = StoreContext::new(store.inner.clone(), &url).expect("store");
        let bytes = reader
            .prefixed
            .get(&object_store::path::Path::from("metadata/v2.metadata.json"))
            .await
            .expect("published version")
            .bytes()
            .await
            .expect("bytes");
        let metadata = TableMetadata::from_json(&bytes).expect("metadata");
        assert_ne!(metadata.current_snapshot_id, Some(10));
        let files = crate::table::files::collect_live_files(&reader, &metadata, 1, |f| {
            Ok(Some(f.file_path))
        })
        .await
        .expect("published files");
        assert_eq!(files.len(), 2);
        if !matches!(fault, CasFault::LostAck) {
            let source = match &error {
                DataFusionError::External(source) => Some(source),
                _ => None,
            }
            .expect("external error");
            let unknown = source
                .downcast_ref::<sail_common_datafusion::error::CommitStateUnknownError>()
                .expect("typed unknown");
            assert!(
                std::error::Error::source(unknown)
                    .expect("original error")
                    .is::<DataFusionError>()
            );
        }
    }
}

#[tokio::test]
async fn bootstrap_pointer_errors_preserve_publication_style_and_artifacts() {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use object_store::path::Path;
    use sail_common_datafusion::error::CommitStateUnknownError;

    use crate::operations::bootstrap::{NewTableMetadataStyle, bootstrap_empty_table_metadata};
    use crate::physical_plan::IcebergCommitExec;
    use crate::physical_plan::action_schema::{
        CommitMeta, encode_commit_meta, iceberg_action_schema,
    };
    use crate::spec::{PartitionSpec, Schema};

    // UUID new-table pointer initialization has a separate planning contract.
    for (style, empty_table_exists) in [
        (NewTableMetadataStyle::Hadoop, false),
        (NewTableMetadataStyle::Hadoop, true),
        (NewTableMetadataStyle::Uuid, true),
    ] {
        for fault in [CasFault::Reject, CasFault::Conflicts(1), CasFault::LostAck] {
            let url = url::Url::parse("memory://bucket/bootstrap-pointer/").expect("URL");
            let store = Arc::new(TestStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )));
            let reader = StoreContext::new(store.inner.clone(), &url).expect("reader");
            let schema = Schema::builder().build().expect("schema");
            let properties = vec![("format-version".to_string(), "2".to_string())];
            let pointer = if empty_table_exists {
                let empty = bootstrap_empty_table_metadata(
                    &url,
                    &reader,
                    schema.clone(),
                    PartitionSpec::unpartitioned_spec(),
                    &properties,
                    style,
                )
                .await
                .expect("empty table");
                url.join(&empty.metadata_file)
                    .expect("pointer URL")
                    .to_string()
            } else {
                String::new()
            };
            let old_pointer = pointer.clone();
            let (store, catalog, ctx, url, mut context) =
                pointer_fixture(fault, store, url, pointer);
            let hadoop = matches!(style, NewTableMetadataStyle::Hadoop);
            if hadoop {
                context.commit = CommitAuthority::Filesystem;
            } else {
                context.pointer = MetadataPointerAuthority::CatalogPropertyCas;
            }
            let actions = encode_commit_meta(CommitMeta {
                table_uri: url.to_string(),
                table_properties: properties,
                lakehouse_table: Some(context),
                schema: Some(schema),
                partition_spec: Some(PartitionSpec::unpartitioned_spec()),
                ..Default::default()
            })
            .expect("commit action");
            let input = MemorySourceConfig::try_new_exec(
                &[vec![actions]],
                iceberg_action_schema().expect("action schema"),
                None,
            )
            .expect("input");
            let commit = IcebergCommitExec::new(
                input,
                url.clone(),
                None,
                crate::operations::SnapshotUpdateKind::FastAppend,
            );
            let error = commit
                .execute(0, ctx)
                .expect("stream")
                .try_collect::<Vec<_>>()
                .await
                .expect_err("pointer failure");
            crate::io::test_store::assert_commit_retryability(
                &error,
                hadoop || matches!(fault, CasFault::LostAck),
            );
            assert_eq!(catalog.attempts.load(Ordering::SeqCst), 1);
            assert!(store.deletes.lock().expect("deletes").is_empty());

            // The classifier must retain the exact typed provider failure through the wrapper.
            let source = match &error {
                DataFusionError::External(source) => Some(source),
                _ => None,
            }
            .expect("external error");
            let original = if hadoop {
                let unknown = source
                    .downcast_ref::<CommitStateUnknownError>()
                    .expect("unknown state");
                let original = std::error::Error::source(unknown)
                    .expect("original error")
                    .downcast_ref::<DataFusionError>()
                    .expect("original DataFusion error");
                match original {
                    DataFusionError::External(original) => Some(original.as_ref()),
                    _ => None,
                }
                .expect("catalog source")
            } else {
                source.as_ref()
            };
            let original = original
                .downcast_ref::<CatalogError>()
                .expect("catalog source");
            assert!(matches!(
                (fault, original),
                (CasFault::Reject, CatalogError::NotSupported(_))
                    | (CasFault::Conflicts(_), CatalogError::Conflict(_))
                    | (CasFault::LostAck, CatalogError::CommitStateUnknown(_))
            ));

            let objects = reader
                .prefixed
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("objects");
            let metadata_files = objects
                .iter()
                .filter(|object| object.location.as_ref().ends_with(".metadata.json"))
                .collect::<Vec<_>>();
            assert_eq!(metadata_files.len(), 1 + usize::from(empty_table_exists));
            let published = metadata_files
                .iter()
                .find(|object| {
                    url.join(object.location.as_ref())
                        .expect("metadata URL")
                        .as_str()
                        != old_pointer
                })
                .expect("new metadata");
            let bytes = reader
                .prefixed
                .get(&published.location)
                .await
                .expect("published metadata")
                .bytes()
                .await
                .expect("bytes");
            let metadata = TableMetadata::from_json(&bytes).expect("readable metadata");
            let snapshot = metadata.current_snapshot().expect("first snapshot");
            if hadoop {
                let latest = crate::table::find_latest_metadata_file(&store.inner, &url)
                    .await
                    .expect("discover published metadata");
                let bytes =
                    crate::table::metadata_loader::load_metadata_file_bytes(&store.inner, &latest)
                        .await
                        .expect("discoverable bytes");
                assert_eq!(
                    TableMetadata::from_json(&bytes)
                        .expect("discoverable metadata")
                        .current_snapshot_id,
                    metadata.current_snapshot_id,
                );
            }
            let manifests = crate::io::load_manifest_list(&reader, snapshot.manifest_list())
                .await
                .expect("manifest list");
            assert!(manifests.entries().is_empty());
            // Empty commits publish a list, metadata and advisory hint, without cleanup.
            assert_eq!(store.puts.load(Ordering::SeqCst), 3);
            assert_eq!(objects.len(), 3 + usize::from(empty_table_exists));
            reader
                .prefixed
                .head(&Path::from("metadata/version-hint.text"))
                .await
                .expect("hint");
            let current_pointer = catalog.pointer.lock().expect("pointer").clone();
            if matches!(fault, CasFault::LostAck) {
                assert_eq!(
                    current_pointer,
                    url.join(published.location.as_ref())
                        .expect("published URL")
                        .to_string()
                );
            } else {
                assert_eq!(current_pointer, old_pointer);
            }
        }
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
