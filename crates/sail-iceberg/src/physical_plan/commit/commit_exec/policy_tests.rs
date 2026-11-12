//! Mock catalog coverage of the shared commit boundary, independent of writer planning.
#![expect(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::lakehouse::{LakehouseCommitOutcome, LakehouseCommitRequest};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::*;
use sail_common_datafusion::catalog::managed::{
    METADATA_LOCATION_UNDERSCORE_KEY, PREVIOUS_METADATA_LOCATION_KEY,
};
use sail_common_datafusion::catalog::*;

use super::*;
use crate::io::test_store::TestStore;
use crate::operations::bootstrap::bootstrap_empty_table_metadata;
use crate::physical_plan::action_schema::{encode_add_data_files, encode_commit_meta};
use crate::spec::types::{NestedField, PrimitiveType};
use crate::spec::{DataFileFormat, FormatVersion};

const LOCK_PROPERTY: &str = "engine.hive.lock-enabled";

struct PolicyCatalog {
    url: Url,
    pointer: Mutex<String>,
    store: Arc<TestStore>,
    enforce_policy: bool,
    gets: AtomicUsize,
    validations: Mutex<Vec<String>>,
    attempts: AtomicUsize,
    publications: AtomicUsize,
    puts_at_conflict: AtomicUsize,
    replacement: Mutex<Option<(String, TableMetadata)>>,
}

fn unsupported<T>() -> CatalogResult<T> {
    Err(CatalogError::NotSupported("unused test operation".into()))
}

#[async_trait::async_trait]
impl CatalogProvider for PolicyCatalog {
    fn get_name(&self) -> &str {
        "test"
    }

    async fn get_table(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(TableStatus {
            catalog: Some("test".into()),
            database: vec!["default".into()],
            name: "items".into(),
            kind: TableKind::Table {
                columns: vec![],
                comment: None,
                constraints: vec![],
                location: Some(self.url.to_string()),
                format: "iceberg".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                // Deliberately omit the policy from the catalog's display properties.
                properties: vec![
                    (
                        METADATA_LOCATION_UNDERSCORE_KEY.into(),
                        self.pointer.lock().expect("pointer").clone(),
                    ),
                    ("table_type".into(), "ICEBERG".into()),
                ],
                is_external: true,
            },
        })
    }

    fn validate_alter_table(&self, options: &AlterTableOptions) -> CatalogResult<()> {
        if let AlterTableOptions::SetTableProperties { properties } = options {
            for (key, value) in properties {
                if key == LOCK_PROPERTY {
                    self.validations
                        .lock()
                        .expect("validations")
                        .push(value.clone());
                    if self.enforce_policy && value != "true" {
                        return Err(CatalogError::NotSupported(format!(
                            "unsupported {LOCK_PROPERTY}={value}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn commit_lakehouse_table(
        &self,
        _: &Namespace,
        _: &str,
        _: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let replacement = self.replacement.lock().expect("replacement").take();
        if let Some((pointer, metadata)) = replacement {
            // A definite native-catalog conflict is a real retry edge in IcebergCommitExec.
            self.store
                .inner
                .put(
                    &Path::from(metadata_location_to_object_path_string(&pointer).expect("path")),
                    metadata.to_json().expect("metadata").into(),
                )
                .await
                .expect("concurrent metadata");
            *self.pointer.lock().expect("pointer") = pointer;
            self.puts_at_conflict
                .store(self.store.puts.load(Ordering::SeqCst), Ordering::SeqCst);
            return Ok(LakehouseCommitOutcome::RetryableConflict {
                message: "policy changed".into(),
            });
        }
        Err(CatalogError::NotSupported(
            "disabled retry must not reach catalog publication".into(),
        ))
    }

    async fn alter_table(
        &self,
        _: &Namespace,
        _: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let AlterTableOptions::SetTableProperties { properties } = options else {
            return unsupported();
        };
        let properties = properties.into_iter().collect::<HashMap<_, _>>();
        let mut pointer = self.pointer.lock().expect("pointer");
        if properties.get(PREVIOUS_METADATA_LOCATION_KEY) != Some(&*pointer) {
            return Err(CatalogError::Conflict(
                "stale expected metadata pointer".into(),
            ));
        }
        *pointer = properties[METADATA_LOCATION_UNDERSCORE_KEY].clone();
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
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

struct Fixture {
    store: Arc<TestStore>,
    catalog: Arc<PolicyCatalog>,
    task: Arc<TaskContext>,
    lakehouse: LakehouseExecutionContext,
    metadata: TableMetadata,
    pointer: String,
    data_file: DataFile,
}

impl Fixture {
    async fn new(with_snapshot: bool, policy: Option<&str>, enforce_policy: bool) -> Self {
        let url = Url::parse("memory:///policy-table/").expect("URL");
        let memory: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let store_ctx = StoreContext::new(memory.clone(), &url).expect("store context");
        let schema = IcebergSchema::builder()
            .with_fields([Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let properties = policy
            .map(|value| vec![(LOCK_PROPERTY.into(), value.into())])
            .unwrap_or_default();
        let bootstrap = bootstrap_empty_table_metadata(
            &url,
            &store_ctx,
            schema,
            PartitionSpec::unpartitioned_spec(),
            &properties,
            NewTableMetadataStyle::Hadoop,
        )
        .await
        .expect("empty table");
        let mut pointer = table_metadata_location(&url, &bootstrap.metadata_file).expect("pointer");
        let session = SessionContext::new();
        session
            .runtime_env()
            .register_object_store(&url, memory.clone());
        if with_snapshot {
            let batch = encode_commit_meta(CommitMeta {
                table_uri: url.to_string(),
                ..Default::default()
            })
            .expect("action");
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)
                    .expect("input");
            IcebergCommitExec::new(input, url.clone(), None, SnapshotUpdateKind::FastAppend)
                .execute(0, session.task_ctx())
                .expect("stream")
                .try_collect::<Vec<_>>()
                .await
                .expect("initial snapshot");
            let path = crate::table::find_latest_metadata_file(&memory, &url)
                .await
                .expect("latest metadata");
            pointer = table_metadata_location(&url, &path).expect("pointer");
        }
        let bytes = load_metadata_file_bytes(
            &memory,
            &metadata_location_to_object_path_string(&pointer).expect("path"),
        )
        .await
        .expect("metadata bytes");
        let metadata = TableMetadata::from_json(&bytes).expect("metadata");
        assert_eq!(metadata.current_snapshot().is_some(), with_snapshot);
        assert_eq!(metadata.format_version, FormatVersion::V2);
        let data_path = url.join("data/writer-owned.parquet").expect("data path");
        memory
            .put(
                &Path::from(data_path.path()),
                Bytes::from_static(b"writer output").into(),
            )
            .await
            .expect("writer output");
        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: data_path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 1,
            file_size_in_bytes: 13,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let store = Arc::new(TestStore::new(memory));
        let catalog = Arc::new(PolicyCatalog {
            url: url.clone(),
            pointer: Mutex::new(pointer.clone()),
            store: store.clone(),
            enforce_policy,
            gets: AtomicUsize::new(0),
            validations: Mutex::new(vec![]),
            attempts: AtomicUsize::new(0),
            publications: AtomicUsize::new(0),
            puts_at_conflict: AtomicUsize::new(0),
            replacement: Mutex::new(None),
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
        let mut lakehouse = LakehouseExecutionContext::catalog_table_context(
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
        lakehouse.commit = CommitAuthority::IcebergMetadataLocationCas;
        Self {
            store,
            catalog,
            task: session.task_ctx(),
            lakehouse,
            metadata,
            pointer,
            data_file,
        }
    }

    fn commit(&self, skip_empty: bool, planned_pointer: Option<&str>) -> IcebergCommitExec {
        // Stale write hints must not override the loaded base's policy.
        let mut table_properties = vec![(LOCK_PROPERTY.into(), "true".into())];
        if let Some(pointer) = planned_pointer {
            table_properties.push((METADATA_LOCATION_UNDERSCORE_KEY.into(), pointer.into()));
        }
        let meta = encode_commit_meta(CommitMeta {
            table_uri: self.catalog.url.to_string(),
            skip_empty_commit: skip_empty,
            table_properties,
            ..Default::default()
        })
        .expect("commit action");
        let mut batches = vec![meta.clone()];
        if !skip_empty {
            batches
                .push(encode_add_data_files(vec![self.data_file.clone()]).expect("writer action"));
        }
        let input =
            MemorySourceConfig::try_new_exec(&[batches], meta.schema(), None).expect("input");
        IcebergCommitExec::new(
            input,
            self.catalog.url.clone(),
            Some(self.lakehouse.clone()),
            SnapshotUpdateKind::FastAppend,
        )
        .with_expected_snapshot_id(Some(
            self.metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
        ))
    }

    async fn inventory(&self) -> HashMap<String, Bytes> {
        let objects = self
            .store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        let mut inventory = HashMap::new();
        for object in objects {
            let bytes = self
                .store
                .inner
                .get(&object.location)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes");
            inventory.insert(object.location.to_string(), bytes);
        }
        inventory
    }

    fn disabled_base(&self) -> (String, TableMetadata) {
        let mut metadata = self.metadata.clone();
        metadata
            .properties
            .insert(LOCK_PROPERTY.into(), "false".into());
        (
            self.catalog
                .url
                .join("metadata/disabled.metadata.json")
                .expect("pointer")
                .to_string(),
            metadata,
        )
    }

    fn assert_policy_error(error: &DataFusionError) {
        assert!(
            error
                .to_string()
                .contains("unsupported engine.hive.lock-enabled=false"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn disabled_loaded_base_rejects_before_first_snapshot_append_or_noop() {
    for with_snapshot in [false, true] {
        for skip_empty in [false, true] {
            let fixture = Fixture::new(with_snapshot, Some("false"), true).await;
            let before = fixture.inventory().await;
            let error = fixture
                .commit(skip_empty, None)
                .execute(0, fixture.task.clone())
                .expect("stream")
                .try_collect::<Vec<_>>()
                .await
                .expect_err("disabled base");
            Fixture::assert_policy_error(&error);
            assert_eq!(fixture.catalog.gets.load(Ordering::SeqCst), 1);
            assert_eq!(
                *fixture.catalog.validations.lock().expect("validations"),
                vec!["false"]
            );
            assert_eq!(fixture.catalog.attempts.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.catalog.publications.load(Ordering::SeqCst), 0);
            assert_eq!(
                *fixture.catalog.pointer.lock().expect("pointer"),
                fixture.pointer
            );
            assert_eq!(fixture.store.puts.load(Ordering::SeqCst), 0);
            assert!(fixture.store.deletes.lock().expect("deletes").is_empty());
            assert_eq!(fixture.inventory().await, before);
        }
    }
}

#[tokio::test]
async fn native_catalog_conflict_revalidates_property_only_change_before_restaging() {
    for with_snapshot in [false, true] {
        let mut fixture = Fixture::new(with_snapshot, Some("true"), true).await;
        fixture.lakehouse.commit = CommitAuthority::IcebergRestCommit;
        let (pointer, metadata) = fixture.disabled_base();
        // Every other field, including snapshot/schema IDs and requirements, is unchanged.
        let mut original = metadata.clone();
        original
            .properties
            .insert(LOCK_PROPERTY.into(), "true".into());
        assert_eq!(
            original.to_json().expect("JSON"),
            fixture.metadata.to_json().expect("JSON")
        );
        let mut expected = fixture.inventory().await;
        expected.insert(
            metadata_location_to_object_path_string(&pointer).expect("path"),
            metadata.to_json().expect("JSON").into(),
        );
        *fixture.catalog.replacement.lock().expect("replacement") =
            Some((pointer.clone(), metadata));
        let error = fixture
            .commit(false, None)
            .execute(0, fixture.task.clone())
            .expect("stream")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("disabled retry base");
        Fixture::assert_policy_error(&error);
        assert_eq!(fixture.catalog.gets.load(Ordering::SeqCst), 2);
        assert_eq!(
            *fixture.catalog.validations.lock().expect("validations"),
            vec!["true", "false"]
        );
        assert_eq!(fixture.catalog.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.catalog.publications.load(Ordering::SeqCst), 0);
        assert_eq!(*fixture.catalog.pointer.lock().expect("pointer"), pointer);
        let puts_at_conflict = fixture.catalog.puts_at_conflict.load(Ordering::SeqCst);
        assert!(puts_at_conflict > 0, "first attempt must prepare manifests");
        assert_eq!(fixture.store.puts.load(Ordering::SeqCst), puts_at_conflict);
        assert_eq!(
            fixture.inventory().await,
            expected,
            "only the concurrent base survives; writer output is preserved"
        );
    }
}

#[tokio::test]
async fn stale_planned_pointer_conflicts_without_reloading_or_replaying() {
    for with_snapshot in [false, true] {
        let fixture = Fixture::new(with_snapshot, Some("true"), true).await;
        let (pointer, metadata) = fixture.disabled_base();
        fixture
            .store
            .inner
            .put(
                &Path::from(metadata_location_to_object_path_string(&pointer).expect("path")),
                metadata.to_json().expect("JSON").into(),
            )
            .await
            .expect("concurrent base");
        *fixture.catalog.pointer.lock().expect("pointer") = pointer.clone();
        let before = fixture.inventory().await;
        let error = fixture
            .commit(false, Some(&fixture.pointer))
            .execute(0, fixture.task.clone())
            .expect("stream")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("stale pointer");
        assert!(
            error
                .to_string()
                .contains("stale expected metadata pointer"),
            "{error}"
        );
        assert_eq!(fixture.catalog.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            *fixture.catalog.validations.lock().expect("validations"),
            vec!["true"]
        );
        assert_eq!(fixture.catalog.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.catalog.publications.load(Ordering::SeqCst), 0);
        assert_eq!(*fixture.catalog.pointer.lock().expect("pointer"), pointer);
        // Both snapshot branches write the advisory hint before catalog CAS. The
        // failed CAS can leave a hint to staged metadata, but not change existing
        // authoritative metadata, manifests, or writer-owned data.
        let after = fixture.inventory().await;
        let hint_path = metadata_location_to_object_path_string(
            fixture
                .catalog
                .url
                .join("metadata/version-hint.text")
                .expect("hint URL")
                .as_str(),
        )
        .expect("hint path");
        for (path, bytes) in &before {
            if path != &hint_path {
                assert_eq!(
                    after.get(path),
                    Some(bytes),
                    "existing object changed: {path}"
                );
            }
        }
        let hint = after.get(&hint_path).expect("advisory hint");
        assert_ne!(Some(hint), before.get(&hint_path));
        let filename = std::str::from_utf8(hint).expect("metadata filename hint");
        assert!(!filename.contains('/'), "hint must be a metadata filename");
        assert_eq!(
            metadata_file_version_from_path(filename),
            metadata_file_version_from_path(&fixture.pointer).map(|version| version + 1),
        );
        let staged_pointer = fixture
            .catalog
            .url
            .join(&format!("metadata/{filename}"))
            .expect("staged URL")
            .to_string();
        assert_ne!(staged_pointer, pointer);
        let staged_path =
            metadata_location_to_object_path_string(&staged_pointer).expect("staged path");
        assert!(!before.contains_key(&staged_path));
        let staged =
            TableMetadata::from_json(after.get(&staged_path).expect("hinted metadata exists"))
                .expect("valid staged metadata");
        assert_eq!(staged.properties[LOCK_PROPERTY], "true");
        assert_ne!(staged.current_snapshot_id, metadata.current_snapshot_id);
        let store_ctx =
            StoreContext::new(fixture.store.clone(), &fixture.catalog.url).expect("store context");
        let staged_files = IcebergCommitExec::current_live_data_files(&store_ctx, &staged)
            .await
            .expect("valid staged manifest graph");
        assert_eq!(staged_files.len(), 1);
        assert_eq!(staged_files[0].file_path, fixture.data_file.file_path);
        assert_eq!(staged_files[0].record_count, fixture.data_file.record_count);
        let authoritative_path =
            metadata_location_to_object_path_string(&pointer).expect("authoritative path");
        let authoritative = TableMetadata::from_json(
            after
                .get(&authoritative_path)
                .expect("authoritative metadata"),
        )
        .expect("valid authoritative metadata");
        assert_eq!(
            serde_json::to_value(&authoritative).expect("metadata"),
            serde_json::to_value(&metadata).expect("metadata")
        );
        assert!(
            IcebergCommitExec::current_live_data_files(&store_ctx, &authoritative)
                .await
                .expect("authoritative live files")
                .is_empty(),
            "staged rows must not become visible"
        );

        // A new attempt without the stale planned pointer must load the disabled
        // catalog base, not the enabled metadata named by the advisory hint.
        let puts = fixture.store.puts.load(Ordering::SeqCst);
        let error = fixture
            .commit(false, None)
            .execute(0, fixture.task.clone())
            .expect("stream")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("disabled authoritative base");
        Fixture::assert_policy_error(&error);
        assert_eq!(fixture.catalog.gets.load(Ordering::SeqCst), 2);
        assert_eq!(
            *fixture.catalog.validations.lock().expect("validations"),
            vec!["true", "false"]
        );
        assert_eq!(fixture.catalog.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.catalog.publications.load(Ordering::SeqCst), 0);
        assert_eq!(*fixture.catalog.pointer.lock().expect("pointer"), pointer);
        assert_eq!(fixture.store.puts.load(Ordering::SeqCst), puts);
        assert_eq!(fixture.inventory().await, after);
    }
}

#[tokio::test]
async fn supported_base_policies_and_non_hms_false_still_publish() {
    for with_snapshot in [false, true] {
        for (policy, enforce_policy) in [(None, true), (Some("true"), true), (Some("false"), false)]
        {
            let fixture = Fixture::new(with_snapshot, policy, enforce_policy).await;
            fixture
                .commit(false, None)
                .execute(0, fixture.task.clone())
                .expect("stream")
                .try_collect::<Vec<_>>()
                .await
                .expect("supported policy");
            assert_eq!(fixture.catalog.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.catalog.publications.load(Ordering::SeqCst), 1);
            assert_ne!(
                *fixture.catalog.pointer.lock().expect("pointer"),
                fixture.pointer
            );
        }
    }
}
