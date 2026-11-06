//! Sail publication-race tests with deterministic storage barriers.
//! Cover immutable metadata versions, stale-base rejection, cancellation, and codec lag.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::{
    CatalogProvider, CreateTableColumnOptions, CreateTableMode, CreateTableOptions, Namespace,
};
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common_datafusion::lakesource::{LakeSource, LakeSourceAlterTableOperation};
use tokio::time::{Duration, timeout};
use url::Url;

use super::SnapshotUpdateKind;
use super::bootstrap::{
    NewTableMetadataStyle, bootstrap_empty_table_metadata, bootstrap_new_table_with_style,
};
use super::replacement::{commit_replacement, prepare_replacement};
use super::replacement_test_support::{ReplacementFault, ReplacementStore};
use crate::datasource::type_converter::arrow_schema_to_iceberg;
use crate::io::StoreContext;
use crate::lake_source::IcebergLakeSource;
use crate::physical_plan::action_schema::{
    CommitMeta, encode_add_data_files, encode_commit_meta, iceberg_action_schema,
};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::physical_plan::commit::commit_exec::IcebergCommitExec;
use crate::spec::{DataFile, PartitionSpec, TableMetadata, TableRequirement};
use crate::table::metadata_loader::{
    find_latest_metadata_file, load_metadata_file_bytes, metadata_file_version_from_path,
};
use crate::utils::metadata::metadata_files_for_version;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Operation = futures::future::BoxFuture<'static, datafusion_common::Result<()>>;
const CODEC: &str = "write.metadata.compression-codec";

#[derive(Clone, Copy, Debug)]
enum Writer {
    Append,
    Replace,
    Alter,
}

struct Fixture {
    url: Url,
    store: Arc<ReplacementStore>,
    context: StoreContext,
    task: Arc<TaskContext>,
    base: TableMetadata,
    base_file: String,
}

async fn data_file(
    store: &StoreContext,
    column: &str,
    id: i32,
    value: i64,
) -> TestResult<DataFile> {
    let field = Field::new(column, DataType::Int64, false)
        .with_metadata(HashMap::from([("PARQUET:field_id".into(), id.to_string())]));
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    let bytes = writer.into_inner()?;
    let size = bytes.len();
    let file = format!("data/{column}-{value}.parquet");
    store
        .prefixed
        .put(&Path::from(file.clone()), Bytes::from(bytes).into())
        .await?;
    Ok(serde_json::from_value(serde_json::json!({
        "content": "DATA", "file_path": format!("memory:///table/{file}"),
        "file_format": "PARQUET", "partition": [], "record_count": 1,
        "file_size_in_bytes": size, "partition_spec_id": 0
    }))?)
}

fn commit(url: &Url, file: DataFile) -> IcebergCommitInfo {
    IcebergCommitInfo {
        snapshot_properties: Default::default(),
        table_uri: url.to_string(),
        row_count: 1,
        action_summary: None,
        data_files: vec![file],
        delete_files: vec![],
        manifest_path: String::new(),
        manifest_list_path: String::new(),
        updates: vec![],
        requirements: vec![],
        table_properties: vec![],
        lakehouse_table: None,
        snapshot_update_kind: SnapshotUpdateKind::FastAppend,
        schema: None,
        partition_spec: None,
    }
}

async fn append(f: &Fixture, value: i64) -> TestResult<Operation> {
    let file = data_file(&f.context, "old", 1, value).await?;
    let schema = iceberg_action_schema()?;
    let actions = datafusion::arrow::compute::concat_batches(
        &schema,
        &[
            encode_add_data_files(vec![file])?,
            encode_commit_meta(CommitMeta {
                table_uri: f.url.to_string(),
                row_count: 1,
                requirements: vec![TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: f.base.current_schema_id,
                }],
                ..Default::default()
            })?,
        ],
    )?;
    let input = MemorySourceConfig::try_new_exec(&[vec![actions]], schema, None)?;
    let exec = IcebergCommitExec::new(input, f.url.clone(), None, SnapshotUpdateKind::FastAppend);
    let task = f.task.clone();
    Ok(Box::pin(async move {
        exec.execute(0, task)?.try_collect::<Vec<_>>().await?;
        Ok(())
    }))
}

async fn fixture(codec: &str, populated: bool) -> TestResult<Fixture> {
    let url = Url::parse("memory:///table/")?;
    let store = Arc::new(ReplacementStore::new(ReplacementFault::BootstrapRace));
    store.disabled.store(true, Ordering::SeqCst);
    let context = StoreContext::new(store.clone(), &url)?;
    let session = SessionContext::new();
    session
        .runtime_env()
        .register_object_store(&url, store.clone());
    let schema = arrow_schema_to_iceberg(&Schema::new(vec![Field::new(
        "old",
        DataType::Int64,
        false,
    )]))?;
    let initial = bootstrap_empty_table_metadata(
        &url,
        &context,
        schema,
        PartitionSpec::unpartitioned_spec(),
        &[(CODEC.into(), codec.into())],
        NewTableMetadataStyle::Hadoop,
    )
    .await?;
    let mut f = Fixture {
        url,
        store,
        context,
        task: session.task_ctx(),
        base: initial.table_metadata,
        base_file: "table/metadata/v1.metadata.json".into(),
    };
    if populated {
        append(&f, 0).await?.await?;
        f.base_file = find_latest_metadata_file(&f.context.base, &f.url).await?;
        f.base = TableMetadata::from_json(
            &load_metadata_file_bytes(&f.context.base, &f.base_file).await?,
        )?;
    }
    f.store.metadata_puts.store(0, Ordering::SeqCst);
    f.store.disabled.store(false, Ordering::SeqCst);
    Ok(f)
}

async fn operation(f: &Fixture, writer: Writer, codec: &str) -> TestResult<Operation> {
    match writer {
        Writer::Append => append(f, 1).await,
        Writer::Alter => {
            let task = f.task.clone();
            let url = f.url.to_string();
            let codec = codec.to_string();
            Ok(Box::pin(async move {
                IcebergLakeSource
                    .alter_table(
                        &task,
                        &url,
                        LakeSourceAlterTableOperation::SetTableProperties {
                            changes: vec![(CODEC.into(), Some(codec))],
                            if_exists: false,
                        },
                        None,
                    )
                    .await?;
                Ok(())
            }))
        }
        Writer::Replace => {
            // Independent coordinators have independent catalog reservations, but share the path.
            let namespace = Namespace::try_from(vec!["db".to_string()])?;
            let provider = Arc::new(MemoryCatalogProvider::new(
                "memory".into(),
                namespace.clone(),
                None,
            ));
            let manager = Arc::new(CatalogManager::try_new(CatalogManagerOptions {
                catalogs: HashMap::from([(
                    "memory".into(),
                    provider.clone() as Arc<dyn CatalogProvider>,
                )]),
                default_catalog: "memory".into(),
                default_database: vec!["db".into()],
                global_temporary_database: vec!["global_temp".into()],
            })?);
            let mut options = CreateTableOptions {
                columns: vec![CreateTableColumnOptions {
                    name: "old".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                }],
                comment: None,
                constraints: vec![],
                location: Some(f.url.to_string()),
                format: "iceberg".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                mode: CreateTableMode::CreateOrReplace,
                properties: vec![],
                is_external: true,
                is_write_precondition: true,
            };
            provider
                .create_table(&namespace, "t", options.clone())
                .await?;
            options.columns[0].name = "replacement".into();
            options.properties = vec![(CODEC.into(), codec.into())];
            let staged = manager.stage_table_replacement(&["t"], options).await?;
            let schema = Schema::new(vec![Field::new("replacement", DataType::Int64, false)]);
            let replacement = prepare_replacement(
                &f.url,
                staged,
                Some(f.base_file.clone()),
                Some(&f.base),
                &schema,
            )?;
            let field_id = replacement
                .metadata
                .current_schema()
                .ok_or("schema")?
                .fields()[0]
                .id;
            let mut commit = commit(
                &f.url,
                data_file(&f.context, "replacement", field_id, 2).await?,
            );
            commit.snapshot_update_kind = SnapshotUpdateKind::FullOverwrite;
            let task =
                SessionContext::new_with_config(SessionConfig::new().with_extension(manager))
                    .task_ctx();
            let url = f.url.clone();
            let context = f.context.clone();
            Ok(Box::pin(async move {
                commit_replacement(&task, &url, &context, replacement, &commit).await
            }))
        }
    }
}

async fn rows(context: &StoreContext, metadata: &TableMetadata) -> TestResult<Vec<i64>> {
    let entries =
        crate::table::files::collect_live_entries(context, metadata, 1, |entry| Ok(Some(entry)))
            .await?;
    let mut values = vec![];
    for entry in entries {
        let url = Url::parse(&entry.data_file.file_path)?;
        let bytes = context
            .base
            .get(&crate::utils::url_to_object_path(&url)?)
            .await?
            .bytes()
            .await?;
        for batch in ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()? {
            let batch = batch?;
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("int64 column")?;
            values.extend(column.values().iter().copied());
        }
    }
    values.sort();
    Ok(values)
}

#[tokio::test]
async fn writers_from_one_base_share_a_codec_key() -> TestResult {
    for populated in [false, true] {
        for (base_codec, requested) in [
            ("none", "gzip"),
            ("gzip", "none"),
            ("none", "none"),
            ("gzip", "gzip"),
        ] {
            for (loser, winner) in [
                (Writer::Append, Writer::Replace),
                (Writer::Replace, Writer::Append),
                (Writer::Replace, Writer::Replace),
                (Writer::Append, Writer::Alter),
                (Writer::Alter, Writer::Append),
            ] {
                timeout(Duration::from_secs(20), async {
                    let f = fixture(base_codec, populated).await?;
                    let next =
                        metadata_file_version_from_path(&f.base_file).ok_or("base version")? + 1;
                    let slow = operation(&f, loser, requested).await?;
                    let fast = operation(&f, winner, requested).await?;
                    let pending = tokio::spawn(slow);
                    f.store.bootstrap_entered.wait().await;
                    fast.await?;
                    let acknowledged_file =
                        find_latest_metadata_file(&f.context.base, &f.url).await?;
                    let suffix = if base_codec == "gzip" {
                        ".gz.metadata.json"
                    } else {
                        ".metadata.json"
                    };
                    assert_eq!(acknowledged_file, format!("table/metadata/v{next}{suffix}"));
                    let acknowledged = f
                        .context
                        .base
                        .get(&Path::from(acknowledged_file.clone()))
                        .await?
                        .bytes()
                        .await?;
                    let winner_metadata = TableMetadata::from_json(
                        &load_metadata_file_bytes(&f.context.base, &acknowledged_file).await?,
                    )?;
                    let expected_rows = match winner {
                        Writer::Replace => vec![2],
                        Writer::Append if populated => vec![0, 1],
                        Writer::Append => vec![1],
                        Writer::Alter if populated => vec![0],
                        Writer::Alter => vec![],
                    };
                    assert_eq!(rows(&f.context, &winner_metadata).await?, expected_rows);
                    let expected_column = if matches!(winner, Writer::Replace) {
                        "replacement"
                    } else {
                        "old"
                    };
                    assert_eq!(
                        winner_metadata
                            .current_schema()
                            .ok_or("winner schema")?
                            .fields()[0]
                            .name,
                        expected_column
                    );
                    f.store.bootstrap_release.wait().await;
                    let result = pending.await?;
                    // Metadata-only ALTER and a compatible ordinary append may re-prepare on conflict.
                    // A first append or replacement must not reuse old-schema outputs on a newer base.
                    let retry_ok = matches!(loser, Writer::Alter)
                        || (populated
                            && matches!((loser, winner), (Writer::Append, Writer::Alter)));
                    assert_eq!(
                        result.is_ok(),
                        retry_ok,
                        "{populated} {base_codec}->{requested} {loser:?}/{winner:?}: {result:?}"
                    );
                    let latest = find_latest_metadata_file(&f.context.base, &f.url).await?;
                    let final_metadata = TableMetadata::from_json(
                        &load_metadata_file_bytes(&f.context.base, &latest).await?,
                    )?;
                    assert_eq!(
                        f.context
                            .base
                            .get(&Path::from(acknowledged_file))
                            .await?
                            .bytes()
                            .await?,
                        acknowledged
                    );
                    assert_eq!(metadata_files_for_version(&f.context, next).await?.len(), 1);
                    assert_eq!(
                        metadata_file_version_from_path(&latest),
                        Some(next + i32::from(retry_ok))
                    );
                    assert_eq!(
                        final_metadata.current_schema(),
                        winner_metadata.current_schema()
                    );
                    if populated && matches!((loser, winner), (Writer::Append, Writer::Alter)) {
                        assert_eq!(rows(&f.context, &final_metadata).await?, vec![0, 1]);
                    } else {
                        assert_eq!(
                            final_metadata.current_snapshot_id,
                            winner_metadata.current_snapshot_id
                        );
                        assert_eq!(rows(&f.context, &final_metadata).await?, expected_rows);
                    }
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })
                .await??;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn competing_creations_share_uncompressed_initial_key() -> TestResult {
    for (slow_codec, fast_codec) in [("none", "gzip"), ("gzip", "none"), ("gzip", "gzip")] {
        timeout(Duration::from_secs(15), async {
            let url = Url::parse("memory:///table/")?;
            let store = Arc::new(ReplacementStore::new(ReplacementFault::BootstrapRace));
            let context = StoreContext::new(store.clone(), &url)?;
            let mut slow = commit(&url, data_file(&context, "slow", 1, 1).await?);
            slow.schema = Some(arrow_schema_to_iceberg(&Schema::new(vec![Field::new(
                "slow",
                DataType::Int64,
                false,
            )]))?);
            slow.table_properties = vec![(CODEC.into(), slow_codec.into())];
            let mut fast = commit(&url, data_file(&context, "fast", 1, 2).await?);
            fast.schema = Some(arrow_schema_to_iceberg(&Schema::new(vec![Field::new(
                "fast",
                DataType::Int64,
                false,
            )]))?);
            fast.table_properties = vec![(CODEC.into(), fast_codec.into())];
            let pending = {
                let url = url.clone();
                let context = context.clone();
                tokio::spawn(async move {
                    bootstrap_new_table_with_style(
                        &url,
                        &context,
                        &slow,
                        NewTableMetadataStyle::Hadoop,
                    )
                    .await
                })
            };
            store.bootstrap_entered.wait().await;
            let winner = bootstrap_new_table_with_style(
                &url,
                &context,
                &fast,
                NewTableMetadataStyle::Hadoop,
            )
            .await?;
            assert_eq!(winner.metadata_file, "metadata/v1.metadata.json");
            let bytes = context
                .prefixed
                .get(&Path::from(winner.metadata_file.clone()))
                .await?
                .bytes()
                .await?;
            store.bootstrap_release.wait().await;
            assert!(pending.await?.is_err());
            assert_eq!(
                find_latest_metadata_file(&context.base, &url).await?,
                "table/metadata/v1.metadata.json"
            );
            assert_eq!(metadata_files_for_version(&context, 1).await?.len(), 1);
            assert_eq!(
                context
                    .prefixed
                    .get(&Path::from(winner.metadata_file))
                    .await?
                    .bytes()
                    .await?,
                bytes
            );
            assert_eq!(rows(&context, &winner.table_metadata).await?, vec![2]);
            assert_eq!(
                winner
                    .table_metadata
                    .properties
                    .get(CODEC)
                    .map(String::as_str),
                Some(fast_codec)
            );
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_after_metadata_put_retains_readable_snapshot() -> TestResult {
    timeout(Duration::from_secs(15), async {
        let url = Url::parse("memory:///table/")?;
        let store = Arc::new(ReplacementStore::new(ReplacementFault::CancelAfterPut));
        let context = StoreContext::new(store.clone(), &url)?;
        let mut info = commit(&url, data_file(&context, "id", 1, 7).await?);
        info.schema = Some(arrow_schema_to_iceberg(&Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]))?);
        info.table_properties = vec![(CODEC.into(), "gzip".into())];
        let pending = {
            let url = url.clone();
            let context = context.clone();
            tokio::spawn(async move {
                bootstrap_new_table_with_style(&url, &context, &info, NewTableMetadataStyle::Hadoop)
                    .await
            })
        };
        store.bootstrap_entered.wait().await;
        pending.abort();
        assert!(pending.await.is_err_and(|error| error.is_cancelled()));
        let latest = find_latest_metadata_file(&context.base, &url).await?;
        let metadata =
            TableMetadata::from_json(&load_metadata_file_bytes(&context.base, &latest).await?)?;
        assert_eq!(latest, "table/metadata/v1.metadata.json");
        assert_eq!(rows(&context, &metadata).await?, vec![7]);
        assert!(
            context
                .prefixed
                .head(&Path::from("metadata/version-hint.text"))
                .await
                .is_err()
        );
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
