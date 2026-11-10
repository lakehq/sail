use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result, plan_err};
use datafusion::execution::TaskContext;
use object_store::path::Path;

use super::operation::{PreparedSnapshotOperation, SnapshotOperation};
use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode, catalog_requirements,
};
use crate::io::StoreContext;
use crate::lake_source::{
    catalog_managed_iceberg_from_properties, metadata_location_from_properties,
    resolve_iceberg_metadata_location,
};
use crate::operations::metadata_commit::{
    BaseMetadataCodec, MetadataFileCommit, MetadataFileStyle, MetadataWriteOutcome,
    PreviousMetadataFile, apply_snapshot_updates,
};
use crate::operations::snapshot::CreatedPathCleanup;
use crate::procedure::expire_snapshots::{Expiration, ExpireSnapshots};
use crate::procedure::rewrite_manifests::RewriteManifests;
use crate::spec::TableMetadata;
use crate::table::metadata_loader::{
    load_metadata_file_bytes, metadata_file_version_from_path,
    metadata_location_to_object_path_string,
};
use crate::utils::metadata::metadata_files_for_version;

const MAX_PROCEDURE_COMMIT_RETRIES: usize = 5;

pub(crate) enum ProcedureCommitOperation {
    Snapshot(SnapshotOperation),
    RewriteManifests(RewriteManifests),
    ExpireSnapshots(ExpireSnapshots),
}

impl From<ExpireSnapshots> for ProcedureCommitOperation {
    fn from(operation: ExpireSnapshots) -> Self {
        Self::ExpireSnapshots(operation)
    }
}

impl From<SnapshotOperation> for ProcedureCommitOperation {
    fn from(operation: SnapshotOperation) -> Self {
        Self::Snapshot(operation)
    }
}

impl From<RewriteManifests> for ProcedureCommitOperation {
    fn from(operation: RewriteManifests) -> Self {
        Self::RewriteManifests(operation)
    }
}

pub(crate) async fn commit_snapshot_operation(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
    operation: impl Into<ProcedureCommitOperation>,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let operation = operation.into();
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let store_context = StoreContext::new(object_store.clone(), table_url)?;
    let catalog_table = lakehouse_table.map(|context| context.catalog_table().to_vec());

    for attempt in 1..=MAX_PROCEDURE_COMMIT_RETRIES {
        let catalog_info = match catalog_table.as_ref() {
            Some(table) => IcebergCatalogCommitCoordinator::load_table_info(ctx, table).await?,
            None => CatalogTableInfo::default(),
        };
        let commit_mode =
            IcebergCatalogCommitMode::resolve(lakehouse_table, &catalog_info, table_properties)?;
        if matches!(&operation, ProcedureCommitOperation::ExpireSnapshots(_))
            && !matches!(commit_mode, IcebergCatalogCommitMode::Filesystem)
        {
            return plan_err!(
                "expire_snapshots requires versioned filesystem publication; catalog pointer CAS and REST/catalog-update authorities are not yet verified and are unsupported"
            );
        }
        let recorded_metadata_location = catalog_info
            .metadata_location
            .clone()
            .or_else(|| metadata_location_from_properties(table_properties));
        let metadata_location = if commit_mode.uses_catalog_metadata() {
            resolve_iceberg_metadata_location(
                lakehouse_table,
                recorded_metadata_location.clone(),
                catalog_info.is_catalog_managed_iceberg_table
                    || catalog_managed_iceberg_from_properties(table_properties),
            )?
        } else {
            // A filesystem catalog's pointer is a hint, not the publication authority.
            // In particular, retries must discover the winning metadata version.
            None
        };
        let metadata_file = match metadata_location.as_deref() {
            Some(location) => metadata_location_to_object_path_string(location)?,
            None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
        };
        let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
        let mut metadata = TableMetadata::from_json(&bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut expiration: Option<Expiration> = None;
        let prepared = match &operation {
            ProcedureCommitOperation::ExpireSnapshots(operation) => {
                let prepared = operation
                    .prepare(&store_context, table_url, &metadata)
                    .await?;
                expiration = Some(prepared.expiration);
                PreparedSnapshotOperation {
                    changed: !prepared.updates.is_empty(),
                    requirements: vec![],
                    updates: prepared.updates,
                    manifests: vec![],
                    output: crate::procedure::expire_snapshots::output(
                        output_schema.clone(),
                        [0; 6],
                    )?,
                }
            }
            ProcedureCommitOperation::Snapshot(operation) => {
                operation.prepare(&metadata, output_schema.clone())?
            }
            ProcedureCommitOperation::RewriteManifests(operation) => {
                let prepared = operation
                    .prepare(&store_context, table_url, &metadata, output_schema.clone())
                    .await?;
                PreparedSnapshotOperation {
                    changed: !prepared.updates.is_empty(),
                    requirements: prepared.requirements,
                    updates: prepared.updates,
                    manifests: prepared.manifests,
                    output: prepared.output,
                }
            }
        };
        if !prepared.changed {
            return Ok(prepared.output);
        }

        let requirements = catalog_requirements(&metadata, &prepared.requirements, &[]);
        let updates = prepared.updates.clone();
        let catalog_commit = if commit_mode.uses_catalog_commit() {
            let table = catalog_table.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "missing catalog table for Iceberg procedure commit".to_string(),
                )
            })?;
            let context = lakehouse_table.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing lakehouse context for Iceberg procedure commit".to_string(),
                )
            })?;
            Some((table, context))
        } else {
            None
        };
        let mut cleanup = stage_manifests(&store_context, &prepared.manifests).await?;
        let mut use_metadata_location_fallback = commit_mode.uses_metadata_location_update();
        if let Some((table, context)) = catalog_commit {
            cleanup.publication_started();
            match IcebergCatalogCommitCoordinator::new(ctx, table)
                .commit(context, requirements, updates)
                .await?
            {
                CatalogCommitOutcome::Committed(_) => {
                    cleanup.disarm();
                    return finish_expiration(
                        ctx,
                        table_url,
                        table_properties,
                        lakehouse_table,
                        &store_context,
                        expiration,
                        prepared.output,
                    )
                    .await;
                }
                CatalogCommitOutcome::Conflict => {
                    cleanup.cleanup().await;
                    if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                        continue;
                    }
                    return Err(procedure_commit_conflict());
                }
                CatalogCommitOutcome::NotSupported
                    if matches!(
                        commit_mode,
                        IcebergCatalogCommitMode::CompatibilityCatalogCommit
                    ) =>
                {
                    cleanup.publication_did_not_happen();
                    use_metadata_location_fallback = true;
                }
                CatalogCommitOutcome::NotSupported => {
                    cleanup.cleanup().await;
                    return plan_err!(
                        "Iceberg catalog commit is not supported by the resolved catalog authority"
                    );
                }
            }
        }

        // These checks cannot publish. Await cleanup on failure rather than relying on Drop.
        let local_preparation = async {
            let base_codec = BaseMetadataCodec::capture(Some(&metadata))?;
            let current_version = metadata_file_version_from_path(&metadata_file).unwrap_or(0);
            let next_version = current_version.checked_add(1).ok_or_else(|| {
                DataFusionError::Execution("Iceberg metadata version overflow".into())
            })?;
            if !use_metadata_location_fallback
                && !metadata_files_for_version(&store_context, next_version)
                    .await?
                    .is_empty()
            {
                return Ok(None);
            }
            apply_snapshot_updates(
                table_url,
                &mut metadata,
                &prepared.updates,
                metadata_location.as_deref().map_or(
                    PreviousMetadataFile::ObjectPath(&metadata_file),
                    PreviousMetadataFile::CatalogLocation,
                ),
            )?;
            let style = if use_metadata_location_fallback {
                MetadataFileStyle::Unique
            } else {
                MetadataFileStyle::Versioned(base_codec)
            };
            MetadataFileCommit::prepare(table_url, &metadata, next_version, style).map(Some)
        }
        .await;
        let prepared_metadata = match local_preparation {
            Ok(Some(prepared_metadata)) => prepared_metadata,
            Ok(None) => {
                cleanup.cleanup().await;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
            Err(error) => {
                cleanup.cleanup().await;
                return Err(error);
            }
        };
        // A versioned metadata Create can itself publish, even if its acknowledgement is lost.
        if !use_metadata_location_fallback {
            cleanup.publication_started();
        }
        match prepared_metadata.write(&store_context).await {
            Ok(MetadataWriteOutcome::Written) => {
                if use_metadata_location_fallback {
                    cleanup.register(prepared_metadata.path().clone());
                }
            }
            Ok(MetadataWriteOutcome::Conflict) => {
                cleanup.cleanup().await;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
            Err(error) => {
                if use_metadata_location_fallback {
                    cleanup.cleanup().await;
                }
                return Err(error);
            }
        }
        cleanup.publication_started();
        if let Some(table) = catalog_table.as_ref()
            && (use_metadata_location_fallback
                || matches!(commit_mode, IcebergCatalogCommitMode::Filesystem))
            && let Err(error) = IcebergCatalogCommitCoordinator::new(ctx, table)
                .update_metadata_location(
                    table_properties,
                    recorded_metadata_location.as_deref(),
                    prepared_metadata.location(),
                )
                .await
        {
            // A unique metadata file is not published until its pointer CAS succeeds.
            // All other errors (including unknown commit state) preserve every artifact.
            if use_metadata_location_fallback && is_catalog_conflict(&error) {
                cleanup.cleanup().await;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
            return Err(prepared_metadata.catalog_publication_error(error));
        }
        cleanup.disarm();
        prepared_metadata.write_hint(&store_context).await;
        return finish_expiration(
            ctx,
            table_url,
            table_properties,
            lakehouse_table,
            &store_context,
            expiration,
            prepared.output,
        )
        .await;
    }
    Err(procedure_commit_conflict())
}

async fn finish_expiration(
    ctx: &TaskContext,
    url: &url::Url,
    properties: &[(String, String)],
    table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
    store: &StoreContext,
    expiration: Option<Expiration>,
    output: RecordBatch,
) -> Result<RecordBatch> {
    match expiration {
        None => Ok(output),
        Some(expiration) => {
            let current = async {
                let info = match table {
                    Some(table) => {
                        IcebergCatalogCommitCoordinator::load_table_info(ctx, table.catalog_table())
                            .await?
                    }
                    None => CatalogTableInfo::default(),
                };
                let mode = IcebergCatalogCommitMode::resolve(table, &info, properties)?;
                if !matches!(mode, IcebergCatalogCommitMode::Filesystem) {
                    return plan_err!("Iceberg publication authority changed after expiration; preserving candidate files");
                }
                // Never read a catalog pointer through a table store: catalog publication is
                // unsupported for expiration, including an authority change during refresh.
                let file = crate::table::find_latest_metadata_file(&store.base, url).await?;
                let bytes = load_metadata_file_bytes(&store.base, &file).await?;
                TableMetadata::from_json(&bytes)
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            }
            .await;
            expiration
                .finish(store, url, current, output.schema())
                .await
        }
    }
}

fn is_catalog_conflict(error: &DataFusionError) -> bool {
    matches!(error, DataFusionError::External(error)
        if matches!(error.downcast_ref::<sail_catalog::error::CatalogError>(),
            Some(sail_catalog::error::CatalogError::Conflict(_))))
}

fn procedure_commit_conflict() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg procedure commit failed after {MAX_PROCEDURE_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

async fn stage_manifests(
    store: &StoreContext,
    manifests: &[(String, Vec<u8>)],
) -> Result<CreatedPathCleanup> {
    let mut cleanup = CreatedPathCleanup::new(store.clone());
    for (path, bytes) in manifests {
        let path = Path::from(path.as_str());
        match store
            .prefixed
            .put_opts(
                &path,
                bytes.clone().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => cleanup.register(path),
            Err(error) => {
                cleanup.cleanup().await;
                return Err(error.into());
            }
        }
    }
    Ok(cleanup)
}

#[cfg(test)]
mod catalog_tests;

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::io::test_store::{PutFault, TestStore};
    use crate::procedure::rewrite_manifests::tests::{
        entry, operation, output_schema, rewrite_fixture,
    };
    use crate::spec::FormatVersion;

    fn staged_files() -> Vec<(String, Vec<u8>)> {
        ["one.avro", "two.avro", "snap-list.avro"]
            .into_iter()
            .map(|name| (format!("metadata/{name}"), name.as_bytes().to_vec()))
            .collect()
    }

    #[tokio::test]
    async fn failed_staging_cleans_only_successful_creates_and_preserves_primary_error() {
        for failed in 1..=3 {
            for cleanup_fails in [false, true] {
                let mut injected = TestStore::new(Arc::new(InMemory::new()));
                injected.reject_put_number = Some(failed);
                injected.reject_delete = cleanup_fails.then(|| "two.avro".into());
                let injected = Arc::new(injected);
                let url = url::Url::parse("memory://bucket/table/").expect("URL");
                let store = StoreContext::new(injected.clone(), &url).expect("store");
                let error = stage_manifests(&store, &staged_files())
                    .await
                    .err()
                    .expect("upload failure");
                assert!(error.to_string().contains("injected upload failure"));
                let deletes = injected.deletes.lock().expect("deletes").clone();
                assert_eq!(deletes.len(), failed - 1);
                assert!(
                    !deletes
                        .iter()
                        .any(|path| path.as_ref().ends_with("snap-list.avro"))
                );
                let remaining = injected
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("list");
                assert_eq!(remaining.len(), usize::from(cleanup_fails && failed == 3));
                if cleanup_fails && failed == 3 {
                    assert!(remaining[0].location.as_ref().ends_with("two.avro"));
                }
            }
        }
    }

    #[tokio::test]
    async fn staging_create_collision_does_not_delete_the_existing_object() {
        let memory = Arc::new(InMemory::new());
        let injected = Arc::new(TestStore::new(memory.clone()));
        let url = url::Url::parse("memory://bucket/table/").expect("URL");
        let existing = Path::from("table/metadata/two.avro");
        memory
            .put(&existing, b"other writer".to_vec().into())
            .await
            .expect("existing");
        let store = StoreContext::new(injected.clone(), &url).expect("store");
        assert!(stage_manifests(&store, &staged_files()).await.is_err());
        assert_eq!(
            memory
                .get(&existing)
                .await
                .expect("get")
                .bytes()
                .await
                .expect("bytes")
                .as_ref(),
            b"other writer"
        );
        assert_eq!(
            *injected.deletes.lock().expect("deletes"),
            vec![Path::from("table/metadata/one.avro")]
        );
    }

    #[tokio::test]
    async fn canceled_staging_cleans_acknowledged_paths_only() {
        let mut injected = TestStore::new(Arc::new(InMemory::new()));
        injected.fault = Some(("two.avro".into(), PutFault::Pause));
        let injected = Arc::new(injected);
        let store = StoreContext::new(
            injected.clone(),
            &url::Url::parse("memory://bucket/table/").expect("URL"),
        )
        .expect("store");
        let task = tokio::spawn(async move {
            let _cleanup = stage_manifests(&store, &staged_files()).await;
        });
        injected.paused.notified().await;
        task.abort();
        assert!(task.await.expect_err("canceled").is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if injected
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("list")
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup completed");
        assert_eq!(
            *injected.deletes.lock().expect("deletes"),
            vec![Path::from("table/metadata/one.avro")]
        );
    }

    pub(super) async fn commit_fixture(
        fault: Option<PutFault>,
        reject_list: bool,
        invalid_codec: bool,
    ) -> (Arc<TestStore>, TaskContext, url::Url, TableMetadata) {
        let (store, url, mut metadata, _) = rewrite_fixture(
            FormatVersion::V2,
            vec![
                (0, vec![entry(1, FormatVersion::V2)]),
                (0, vec![entry(2, FormatVersion::V2)]),
            ],
            false,
        )
        .await;
        if invalid_codec {
            metadata
                .properties
                .insert("write.metadata.compression-codec".into(), "invalid".into());
        }
        store
            .prefixed
            .put(
                &Path::from("metadata/v1.metadata.json"),
                metadata.to_json().expect("json").into(),
            )
            .await
            .expect("metadata");
        let mut injected = TestStore::new(store.base);
        injected.fault = fault.map(|fault| ("v2.metadata.json".into(), fault));
        injected.reject_list_after_put = reject_list;
        let injected = Arc::new(injected);
        let ctx = TaskContext::default();
        ctx.runtime_env()
            .register_object_store(&url, injected.clone());
        (injected, ctx, url, metadata)
    }

    #[tokio::test]
    async fn local_prepublication_failures_await_cleanup() {
        for invalid_codec in [false, true] {
            let (injected, ctx, url, _) = commit_fixture(None, !invalid_codec, invalid_codec).await;
            let before = injected
                .inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("list");
            let error =
                commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema())
                    .await
                    .expect_err("local failure");
            assert!(
                error.to_string().contains(if invalid_codec {
                    "codec"
                } else {
                    "injected listing failure"
                }),
                "{error}"
            );
            assert_eq!(
                injected
                    .inner
                    .list(None)
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("list"),
                before
            );
            assert_eq!(injected.deletes.lock().expect("deletes").len(), 2);
        }
    }

    #[tokio::test]
    async fn metadata_collision_without_readback_preserves_artifacts_and_is_terminal() {
        let (injected, ctx, url, _) = commit_fixture(Some(PutFault::Collision), false, false).await;
        let before = injected
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        let error = commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema())
            .await
            .expect_err("unresolved collision");
        assert!(error.to_string().contains("injected Create collision"));
        crate::io::test_store::assert_commit_retryability(&error, true);
        assert_eq!(injected.puts.load(Ordering::SeqCst), 3);
        let deletes = injected.deletes.lock().expect("deletes").clone();
        assert!(deletes.is_empty());
        assert!(
            deletes
                .iter()
                .all(|path| !path.as_ref().ends_with(".metadata.json"))
        );
        assert_eq!(
            injected
                .inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("list")
                .len(),
            before.len() + 2
        );
    }

    #[tokio::test]
    async fn metadata_publication_errors_preserve_artifacts_including_lost_ack() {
        for fault in [PutFault::Reject, PutFault::LostAckUnreadable] {
            let (injected, ctx, url, before) = commit_fixture(Some(fault), false, false).await;
            let error =
                commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema())
                    .await
                    .expect_err("publication error");
            assert!(error.to_string().contains("injected"));
            crate::io::test_store::assert_commit_retryability(&error, true);
            let source = match &error {
                DataFusionError::External(source) => Some(source),
                _ => None,
            }
            .expect("external error");
            let unknown = source
                .downcast_ref::<sail_common_datafusion::error::CommitStateUnknownError>()
                .expect("typed unknown");
            let original = std::error::Error::source(unknown).expect("original error");
            assert!(original.is::<object_store::Error>());
            assert!(
                original
                    .source()
                    .expect("I/O source")
                    .is::<std::io::Error>()
            );
            assert_eq!(
                injected.puts.load(Ordering::SeqCst),
                3,
                "one publication attempt"
            );
            assert!(injected.deletes.lock().expect("deletes").is_empty());
            let store = StoreContext::new(injected.inner.clone(), &url).expect("store");
            let metadata = store
                .prefixed
                .get(&Path::from("metadata/v2.metadata.json"))
                .await;
            if matches!(fault, PutFault::LostAckUnreadable) {
                let metadata = TableMetadata::from_json(
                    &metadata
                        .expect("published metadata")
                        .bytes()
                        .await
                        .expect("bytes"),
                )
                .expect("metadata");
                assert_ne!(metadata.current_snapshot_id, before.current_snapshot_id);
                let files = crate::table::files::collect_live_files(&store, &metadata, 1, |file| {
                    Ok(Some(file.file_path))
                })
                .await
                .expect("published files are readable");
                assert_eq!(files.len(), 2);
            } else {
                assert!(metadata.is_err());
            }
        }
    }

    #[tokio::test]
    async fn exact_versioned_readback_publishes_once_and_preserves_rewritten_files() {
        let (injected, ctx, url, before) =
            commit_fixture(Some(PutFault::LostAck), false, false).await;
        let output = commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema())
            .await
            .expect("exact readback resolves publication");
        assert_eq!(output.num_rows(), 1);
        assert_eq!(injected.puts.load(Ordering::SeqCst), 4);
        assert!(injected.deletes.lock().expect("deletes").is_empty());
        let reader = StoreContext::new(injected.inner.clone(), &url).expect("reader");
        let bytes = reader
            .prefixed
            .get(&Path::from("metadata/v2.metadata.json"))
            .await
            .expect("published metadata")
            .bytes()
            .await
            .expect("bytes");
        let metadata = TableMetadata::from_json(&bytes).expect("metadata");
        assert_ne!(metadata.current_snapshot_id, before.current_snapshot_id);
        let files = crate::table::files::collect_live_files(&reader, &metadata, 1, |file| {
            Ok(Some(file.file_path))
        })
        .await
        .expect("readable files");
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn observed_versioned_create_survives_cross_codec_collision_and_listing_failure() {
        use sail_common_datafusion::error::CommitStateUnknownError;

        for listing_failure in [false, true] {
            let (injected, ctx, url, before) =
                commit_fixture(Some(PutFault::PauseAfterWrite), false, false).await;
            let reader_store =
                StoreContext::new(injected.inner.clone(), &url).expect("reader store");
            let task = tokio::spawn(async move {
                commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema()).await
            });
            injected.paused.notified().await;
            // Create has published, but the committer has not done its post-create listing.
            let own_path = Path::from("metadata/v2.metadata.json");
            let observed = reader_store
                .prefixed
                .get(&own_path)
                .await
                .expect("discoverable metadata")
                .bytes()
                .await
                .expect("metadata bytes");
            let metadata = TableMetadata::from_json(&observed).expect("reader metadata");
            assert_ne!(metadata.current_snapshot_id, before.current_snapshot_id);
            let files_before =
                crate::table::files::collect_live_files(&reader_store, &metadata, 1, |file| {
                    Ok(Some(file.file_path))
                })
                .await
                .expect("reader follows published manifests");
            assert_eq!(files_before.len(), 2);
            if listing_failure {
                injected.reject_list.store(true, Ordering::SeqCst);
            } else {
                let other_path = "metadata/v2.gz.metadata.json";
                let bytes = crate::table::metadata_loader::encode_metadata_file(
                    other_path,
                    &before.to_json().expect("metadata"),
                )
                .expect("gzip metadata");
                reader_store
                    .prefixed
                    .put_opts(
                        &Path::from(other_path),
                        bytes.into(),
                        object_store::PutOptions {
                            mode: object_store::PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("competing codec publication");
            }
            injected.resume.notify_one();
            let error = task
                .await
                .expect("commit task")
                .expect_err("uncertain publication");
            assert!(
                matches!(&error, DataFusionError::External(source) if source.downcast_ref::<CommitStateUnknownError>().is_some()),
                "{error}"
            );
            crate::io::test_store::assert_commit_retryability(&error, true);
            assert_eq!(
                injected.puts.load(Ordering::SeqCst),
                3,
                "must not retry uncertain publication"
            );
            assert!(injected.deletes.lock().expect("deletes").is_empty());
            assert_eq!(
                reader_store
                    .prefixed
                    .get(&own_path)
                    .await
                    .expect("own metadata preserved")
                    .bytes()
                    .await
                    .expect("bytes"),
                observed
            );
            let files_after =
                crate::table::files::collect_live_files(&reader_store, &metadata, 1, |file| {
                    Ok(Some(file.file_path))
                })
                .await
                .expect("observed snapshot remains readable");
            assert_eq!(files_after, files_before);
        }
    }

    #[tokio::test]
    async fn canceled_metadata_publication_preserves_staged_manifests() {
        let (injected, ctx, url, _) = commit_fixture(Some(PutFault::Pause), false, false).await;
        let before = injected
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .len();
        let task = tokio::spawn(async move {
            commit_snapshot_operation(&ctx, &url, &[], None, operation(), output_schema()).await
        });
        injected.paused.notified().await;
        task.abort();
        assert!(task.await.expect_err("canceled").is_cancelled());
        tokio::task::yield_now().await;
        assert!(injected.deletes.lock().expect("deletes").is_empty());
        assert_eq!(
            injected
                .inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("list")
                .len(),
            before + 2
        );
    }
}
