use std::sync::Arc;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::object_store::ObjectStore;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;

/// Planning-only Parquet metadata attached to a `PartitionedFile`.
///
/// File repartitioning clones these extensions. The optimizer uses the offsets
/// to replace byte splits with whole row groups, then emits ordinary file ranges.
/// Workers do not need this metadata or a custom scan codec.
#[derive(Debug)]
pub struct ParquetScanMetadata {
    pub row_groups: Vec<ParquetRowGroup>,
}

#[derive(Debug)]
pub struct ParquetRowGroup {
    /// The first column's dictionary/data page offset, as used by Parquet range pruning.
    pub offset: i64,
    pub compressed_size: u64,
    pub num_rows: u64,
}

/// Retain the footer metadata needed for partition planning, using the same cache as scan readers.
pub async fn load_parquet_scan_metadata(
    file_groups: Vec<FileGroup>,
    store: &Arc<dyn ObjectStore>,
    metadata_cache: &Arc<FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    concurrency: usize,
) -> Result<Vec<FileGroup>> {
    let (mut files, statistics): (Vec<_>, Vec<_>) = file_groups
        .into_iter()
        .map(|group| {
            let statistics = group.file_statistics(None).cloned().map(Arc::new);
            (group.into_inner(), statistics)
        })
        .unzip();

    futures::stream::iter(files.iter_mut().flatten().map(Ok))
        .try_for_each_concurrent(concurrency, |file: &mut PartitionedFile| async move {
            let metadata = DFParquetMetadata::new(store.as_ref(), &file.object_meta)
                .with_metadata_size_hint(file.metadata_size_hint.or(metadata_size_hint))
                .with_file_metadata_cache(Some(Arc::clone(metadata_cache)))
                .fetch_metadata()
                .await?;
            let row_groups = metadata
                .row_groups()
                .iter()
                .map(|group| {
                    let column = group.columns().first()?;
                    Some(ParquetRowGroup {
                        offset: column
                            .dictionary_page_offset()
                            .unwrap_or_else(|| column.data_page_offset()),
                        compressed_size: u64::try_from(group.compressed_size()).ok()?,
                        num_rows: u64::try_from(group.num_rows()).ok()?,
                    })
                })
                .collect::<Option<Vec<_>>>();
            if let Some(row_groups) = row_groups {
                file.extensions.insert(ParquetScanMetadata { row_groups });
            }
            Ok::<_, DataFusionError>(())
        })
        .await?;

    Ok(files
        .into_iter()
        .zip(statistics)
        .map(|(files, statistics)| {
            let mut group = FileGroup::new(files);
            if let Some(statistics) = statistics {
                group = group.with_statistics(statistics);
            }
            group
        })
        .collect())
}
