// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
            let merged = Schema::try_merge(vec![p.as_ref().clone(), j.as_ref().clone()])?;
            Arc::new(merged)
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
