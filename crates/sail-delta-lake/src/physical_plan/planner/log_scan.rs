use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource as _};
use datafusion::datasource::schema_adapter::DefaultSchemaAdapterFactory;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::path::{Path, DELIMITER};
use object_store::{ObjectMeta, ObjectStore};

use super::context::PlannerContext;
use crate::datasource::create_object_store_url;

const DELTA_LOG_DIR: &str = "_delta_log";

fn parse_log_version_prefix(filename: &str) -> Option<u64> {
    // Delta log files are typically named with a 20-digit version prefix:
    // - commits:     00000000000000000010.json
    // - checkpoints: 00000000000000000010.checkpoint.parquet
    //
    // For multipart checkpoints, we still take the leading version prefix.
    let prefix = filename.get(0..20)?;
    if !prefix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    prefix.parse::<u64>().ok()
}

fn log_file_path(table_root_path: &str, filename: &str) -> Path {
    // Object store paths are absolute for local filesystem stores in our setup (DataFusion uses
    // `ObjectStoreUrl::local_filesystem()`).
    Path::from(format!(
        "{}{}{}{}{}",
        table_root_path, DELIMITER, DELTA_LOG_DIR, DELIMITER, filename
    ))
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
                store
                    .head(&p)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await
}

fn to_partitioned_files(metas: Vec<ObjectMeta>) -> Vec<PartitionedFile> {
    metas
        .into_iter()
        .map(|m| PartitionedFile {
            object_meta: m,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        })
        .collect()
}

fn to_file_groups(metas: Vec<ObjectMeta>, target_partitions: usize) -> Vec<FileGroup> {
    let target_partitions = target_partitions.max(1);
    if metas.is_empty() {
        return vec![];
    }

    // Ensure deterministic file group ordering for stable EXPLAIN snapshots.
    // `head_many(...).buffer_unordered(...)` is intentionally concurrent, so we sort by path
    // before chunking into FileGroups.
    let mut metas = metas;
    metas.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

    let mut files = to_partitioned_files(metas);
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
    groups
}

/// Build a `UnionExec` over `_delta_log` checkpoint parquet + commit json files using DataFusion's
/// `DataSourceExec`. Returns `(plan, checkpoint_filenames, commit_filenames)` for observability.
pub async fn build_delta_log_datasource_union(
    ctx: &PlannerContext<'_>,
    checkpoint_files: Vec<String>,
    commit_files: Vec<String>,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<String>, Vec<String>)> {
    let store = ctx.object_store()?;
    let log_store = ctx.log_store()?;
    let object_store_url = create_object_store_url(&log_store.config().location)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Avoid double-counting actions that are already materialized into the checkpoint:
    // only scan commit JSONs strictly newer than the latest checkpoint version.
    let latest_checkpoint_version = checkpoint_files
        .iter()
        .filter_map(|f| parse_log_version_prefix(f))
        .max();
    let commit_files = if let Some(cp_ver) = latest_checkpoint_version {
        commit_files
            .into_iter()
            .filter(|f| {
                parse_log_version_prefix(f)
                    .map(|v| v > cp_ver)
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>()
    } else {
        commit_files
    };

    let table_root_path = log_store.config().location.path();
    let (checkpoint_metas, commit_metas) = tokio::try_join!(
        head_many(&store, table_root_path, &checkpoint_files),
        head_many(&store, table_root_path, &commit_files)
    )?;

    // Infer schemas (best-effort). If there are no files for either side, we still build an empty
    // scan of the other side.
    let parquet_schema = if checkpoint_metas.is_empty() {
        None
    } else {
        Some(
            ParquetFormat::default()
                .infer_schema(ctx.session(), &store, &checkpoint_metas)
                .await?,
        )
    };
    let json_schema = if commit_metas.is_empty() {
        None
    } else {
        Some(
            JsonFormat::default()
                .infer_schema(ctx.session(), &store, &commit_metas)
                .await?,
        )
    };

    let merged = match (parquet_schema, json_schema) {
        (Some(p), Some(j)) => {
            // The inferred JSON schema may disagree with the checkpoint parquet schema for
            // map-like fields (e.g. `add.partitionValues`). Prefer a stable schema to avoid
            // planning failures during EXPLAIN.
            match Schema::try_merge(vec![p.as_ref().clone(), j.as_ref().clone()]) {
                Ok(merged) => Arc::new(merged),
                Err(_) => p,
            }
        }
        (Some(p), None) => p,
        (None, Some(j)) => j,
        (None, None) => {
            return Err(DataFusionError::Plan(
                "no _delta_log files found to build log scan".to_string(),
            ))
        }
    };

    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let target_partitions = ctx.session().config().target_partitions();

    if !checkpoint_metas.is_empty() {
        let source = datafusion::datasource::physical_plan::ParquetSource::default()
            .with_schema_adapter_factory(Arc::new(DefaultSchemaAdapterFactory {}))?;
        let groups = to_file_groups(checkpoint_metas, target_partitions);
        let conf =
            FileScanConfigBuilder::new(object_store_url.clone(), Arc::clone(&merged), source)
                .with_file_groups(groups)
                .build();
        inputs.push(DataSourceExec::from_data_source(conf));
    }

    if !commit_metas.is_empty() {
        let source = Arc::new(datafusion::datasource::physical_plan::JsonSource::new());
        let groups = to_file_groups(commit_metas, target_partitions);
        let conf = FileScanConfigBuilder::new(object_store_url, Arc::clone(&merged), source)
            .with_file_groups(groups)
            .build();
        inputs.push(DataSourceExec::from_data_source(conf));
    }

    Ok((UnionExec::try_new(inputs)?, checkpoint_files, commit_files))
}
