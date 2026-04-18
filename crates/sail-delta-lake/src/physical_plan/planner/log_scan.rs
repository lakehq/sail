use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::{Path, DELIMITER};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use super::context::PlannerContext;
use crate::datasource::create_object_store_url;
use crate::physical_plan::COL_LOG_VERSION;
use crate::spec::{
    add_struct_type, delta_log_file_path, metadata_struct_type, parse_version_prefix,
    protocol_struct_type, remove_struct_type, transaction_struct_type,
};

/// The canonical Delta log file schema with proper Map types for fields like `partitionValues`.
///
/// JSON schema inference gives `partitionValues` as a Struct, which breaks `map_extract`.
/// By using this fixed schema for JSON-only log reads (when no parquet checkpoint exists),
/// we ensure consistent Map types regardless of whether a checkpoint is present.
static DELTA_LOG_FILE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    fn to_arrow(st: crate::spec::StructType) -> DataType {
        #[expect(clippy::expect_used)]
        DataType::try_from(&crate::spec::DataType::from(st))
            .expect("spec struct type should convert to Arrow DataType")
    }
    Arc::new(Schema::new(vec![
        Field::new("add", to_arrow(add_struct_type()), true),
        Field::new("remove", to_arrow(remove_struct_type()), true),
        Field::new("metaData", to_arrow(metadata_struct_type()), true),
        Field::new("protocol", to_arrow(protocol_struct_type()), true),
        Field::new("txn", to_arrow(transaction_struct_type()), true),
    ]))
});

#[derive(Debug, Clone, Default)]
pub struct LogScanOptions {
    /// Optional projection of top-level log columns (e.g. ["add", "remove", "metaData"]).
    ///
    /// When set, the scan will only read these columns plus any required partition columns.
    pub projection: Option<Vec<String>>,
    /// Optional pushdown predicate for checkpoint parquet scans.
    pub parquet_predicate: Option<Arc<dyn PhysicalExpr>>,
}

fn parse_log_version_prefix(filename: &str) -> Option<u64> {
    parse_version_prefix(filename)?.try_into().ok()
}

fn log_file_path(table_root_path: &str, filename: &str) -> Path {
    // Object store paths are absolute for local filesystem stores in our setup (DataFusion uses
    // `ObjectStoreUrl::local_filesystem()`).
    delta_log_file_path(table_root_path, filename)
}

async fn head_many(
    store: &Arc<dyn ObjectStore>,
    table_root_path: &str,
    files: &[String],
) -> Result<Vec<ObjectMeta>> {
    if files.is_empty() {
        return Ok(vec![]);
    }

    // Concurrency is intentionally bounded to avoid overwhelming object stores.
    let concurrency = std::cmp::min(64usize, files.len());
    stream::iter(files.iter().cloned())
        .map(|f| {
            let store = Arc::clone(store);
            let p = log_file_path(table_root_path, &f);
            async move {
                store.head(&p).await.map_err(|e| {
                    DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(e))
                })
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await
}

fn to_partitioned_files(metas: Vec<ObjectMeta>) -> Result<Vec<PartitionedFile>> {
    metas
        .into_iter()
        .map(|m| {
            let loc = m.location.as_ref();
            let filename = loc.rsplit(DELIMITER).next().unwrap_or(loc);
            let ver = parse_log_version_prefix(filename).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to parse delta log version from file '{filename}'"
                ))
            })?;
            let ver = i64::try_from(ver).map_err(|_| {
                DataFusionError::Plan(format!(
                    "delta log version '{ver}' does not fit into Int64 for file '{filename}'"
                ))
            })?;
            Ok(PartitionedFile {
                object_meta: m,
                partition_values: vec![ScalarValue::Int64(Some(ver))],
                range: None,
                statistics: None,
                ordering: None,
                extensions: None,
                metadata_size_hint: None,
            })
        })
        .collect()
}

/// Create partitioned files from V2 sidecar file metadata, assigning a fixed checkpoint version
/// as the partition value since sidecar filenames do not carry a version prefix.
fn to_partitioned_files_with_version(
    metas: Vec<ObjectMeta>,
    version: i64,
) -> Result<Vec<PartitionedFile>> {
    Ok(metas
        .into_iter()
        .map(|m| PartitionedFile {
            object_meta: m,
            partition_values: vec![ScalarValue::Int64(Some(version))],
            range: None,
            statistics: None,
            ordering: None,
            extensions: None,
            metadata_size_hint: None,
        })
        .collect())
}

fn to_file_groups(metas: Vec<ObjectMeta>, target_partitions: usize) -> Result<Vec<FileGroup>> {
    let target_partitions = target_partitions.max(1);
    if metas.is_empty() {
        return Ok(vec![]);
    }

    // Ensure deterministic file group ordering for stable EXPLAIN snapshots.
    // `head_many(...).buffer_unordered(...)` is intentionally concurrent, so we sort by path
    // before chunking into FileGroups.
    let mut metas = metas;
    metas.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

    let mut files = to_partitioned_files(metas)?;
    let num_groups = std::cmp::min(target_partitions, files.len());
    let chunk_size = files.len().div_ceil(num_groups);

    let mut groups = Vec::with_capacity(num_groups);
    while !files.is_empty() {
        let rest = if files.len() > chunk_size {
            files.split_off(chunk_size)
        } else {
            Vec::new()
        };
        groups.push(FileGroup::from(std::mem::take(&mut files)));
        files = rest;
    }
    Ok(groups)
}

pub async fn build_delta_log_datasource_scans_with_options(
    ctx: &PlannerContext<'_>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    sidecar_files: Vec<String>,
    options: LogScanOptions,
) -> Result<(
    Option<Arc<dyn ExecutionPlan>>,
    Option<Arc<dyn ExecutionPlan>>,
    Vec<String>,
    Vec<String>,
)> {
    let store = ctx.object_store()?;
    let log_store = ctx.log_store()?;
    let object_store_url = create_object_store_url(&log_store.config().location).map_err(|e| {
        DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(e))
    })?;

    // Commit/checkpoint lists are expected to be selected by the planner log-segment resolver.
    // This builder only materializes datasource scans from those resolved filenames.
    let table_root_path = log_store.config().location.path();
    let (checkpoint_metas, commit_metas, sidecar_metas) = tokio::try_join!(
        head_many(&store, table_root_path, &checkpoint_files),
        head_many(&store, table_root_path, &commit_files),
        head_many(&store, table_root_path, &sidecar_files)
    )?;

    // Resolve the checkpoint version for assigning to sidecar partition values.
    let checkpoint_version = checkpoint_files
        .first()
        .and_then(|f| parse_log_version_prefix(f))
        .map(|v| v as i64);

    // Infer schemas for parquet checkpoint files only. JSON commit files use the canonical
    // Delta log file schema (see `DELTA_LOG_FILE_SCHEMA`) to avoid type mismatches for
    // map-like fields (e.g. `add.partitionValues`).
    //
    // For V2 checkpoints with sidecars, include sidecar files in schema inference since they
    // share the same `CheckpointActionRow` encoding.
    let all_parquet_metas: Vec<ObjectMeta> = checkpoint_metas
        .iter()
        .chain(sidecar_metas.iter())
        .cloned()
        .collect();
    let parquet_schema = if all_parquet_metas.is_empty() {
        None
    } else {
        Some(
            ParquetFormat::default()
                .infer_schema(ctx.session(), &store, &all_parquet_metas)
                .await?,
        )
    };
    let has_commit_files = !commit_metas.is_empty();

    let merged = match (parquet_schema, has_commit_files) {
        (Some(p), _) => {
            // When a checkpoint (parquet) file is present, prefer its schema. The parquet
            // checkpoint schema has proper Map types for fields like `add.partitionValues`,
            // while JSON inference yields Struct types which are incompatible with `map_extract`.
            p
        }
        (None, true) => {
            // No parquet checkpoint exists (e.g. before the first checkpoint interval fires).
            // Use the canonical Delta log file schema so that `partitionValues` and other
            // map-like fields have the correct Map Arrow type instead of an inferred Struct.
            Arc::clone(&*DELTA_LOG_FILE_SCHEMA)
        }
        (None, false) => {
            return Err(DataFusionError::Plan(
                "no _delta_log files found to build log scan".to_string(),
            ))
        }
    };

    let projection_indices = if let Some(projection) = &options.projection {
        let mut projection = projection.clone();
        if !projection.iter().any(|col| col == COL_LOG_VERSION) {
            projection.push(COL_LOG_VERSION.to_string());
        }
        let file_schema_len = merged.fields().len();
        let mut indices = Vec::with_capacity(projection.len());
        for col in &projection {
            if col == COL_LOG_VERSION {
                indices.push(file_schema_len);
                continue;
            }

            match merged.index_of(col) {
                Ok(idx) => indices.push(idx),
                Err(_) => {
                    // Some Delta writers/checkpoint formats may omit an action column
                    // (for example, no `remove` records in a newly created table).
                    // Skip missing projected columns and let downstream replay logic
                    // treat them as absent.
                }
            }
        }
        Some(indices)
    } else {
        None
    };

    let target_partitions = ctx.session().config().target_partitions();
    let table_schema = TableSchema::new(
        Arc::clone(&merged),
        vec![Arc::new(Field::new(
            COL_LOG_VERSION,
            DataType::Int64,
            false,
        ))],
    );

    let checkpoint_scan: Option<Arc<dyn ExecutionPlan>> = if checkpoint_metas.is_empty() {
        None
    } else {
        let mut source =
            datafusion::datasource::physical_plan::ParquetSource::new(table_schema.clone());
        if let Some(predicate) = &options.parquet_predicate {
            source = source.with_predicate(Arc::clone(predicate));
        }
        let source: Arc<dyn datafusion::datasource::physical_plan::FileSource> = Arc::new(source);
        // For V2 checkpoints, include sidecar files alongside the main checkpoint.
        // Both use the same CheckpointActionRow parquet schema.
        let mut all_checkpoint_files = to_partitioned_files(checkpoint_metas)?;
        if let Some(cp_version) = checkpoint_version {
            let sidecar_partitioned = to_partitioned_files_with_version(sidecar_metas, cp_version)?;
            all_checkpoint_files.extend(sidecar_partitioned);
        }
        all_checkpoint_files.sort_by(|a, b| {
            a.object_meta
                .location
                .as_ref()
                .cmp(b.object_meta.location.as_ref())
        });
        let num_groups = std::cmp::min(target_partitions.max(1), all_checkpoint_files.len().max(1));
        let chunk_size = all_checkpoint_files.len().div_ceil(num_groups);
        let mut groups = Vec::with_capacity(num_groups);
        while !all_checkpoint_files.is_empty() {
            let rest = if all_checkpoint_files.len() > chunk_size {
                all_checkpoint_files.split_off(chunk_size)
            } else {
                Vec::new()
            };
            groups.push(FileGroup::from(std::mem::take(&mut all_checkpoint_files)));
            all_checkpoint_files = rest;
        }
        let conf = FileScanConfigBuilder::new(object_store_url.clone(), source)
            .with_file_groups(groups)
            .with_projection_indices(projection_indices.clone())?
            .build();
        Some(DataSourceExec::from_data_source(conf))
    };

    let commit_scan: Option<Arc<dyn ExecutionPlan>> = if commit_metas.is_empty() {
        None
    } else {
        let source: Arc<dyn datafusion::datasource::physical_plan::FileSource> = Arc::new(
            datafusion::datasource::physical_plan::JsonSource::new(table_schema),
        );
        let groups = to_file_groups(commit_metas, target_partitions)?;
        let conf = FileScanConfigBuilder::new(object_store_url, source)
            .with_file_groups(groups)
            .with_projection_indices(projection_indices)?
            .build();
        Some(DataSourceExec::from_data_source(conf))
    };

    Ok((checkpoint_scan, commit_scan, checkpoint_files, commit_files))
}

#[expect(dead_code)]
pub async fn build_delta_log_datasource_union_with_options(
    ctx: &PlannerContext<'_>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
    options: LogScanOptions,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<String>, Vec<String>)> {
    let (checkpoint_scan, commit_scan, checkpoint_files, commit_files) =
        build_delta_log_datasource_scans_with_options(
            ctx,
            checkpoint_files,
            commit_files,
            vec![],
            options,
        )
        .await?;

    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    if let Some(cp) = checkpoint_scan {
        inputs.push(cp);
    }
    if let Some(c) = commit_scan {
        inputs.push(c);
    }

    Ok((UnionExec::try_new(inputs)?, checkpoint_files, commit_files))
}
