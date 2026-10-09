use std::sync::Arc;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    CachedParquetFileReaderFactory as DataFusionReaderFactory, ParquetFileReaderFactory,
};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::{DataFusionError, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use sail_common_datafusion::scan::{ParquetRowGroup, ParquetScanMetadata};

use crate::file_metadata_cache::MokaFileMetadataCache;

/// Bind each reader's cache to the same file version used during footer preparation.
#[derive(Debug)]
pub struct CachedParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<MokaFileMetadataCache>,
}

impl CachedParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, metadata_cache: Arc<MokaFileMetadataCache>) -> Self {
        Self {
            store,
            metadata_cache,
        }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let cache = Arc::new(self.metadata_cache.for_file(&file.object_meta));
        DataFusionReaderFactory::new(Arc::clone(&self.store), cache).create_reader(
            partition_index,
            file,
            metadata_size_hint,
            metrics,
        )
    }
}

/// Retain the footer metadata needed for partition planning, using the same cache as scan readers.
pub async fn load_parquet_scan_metadata(
    file_groups: Vec<FileGroup>,
    store: &Arc<dyn ObjectStore>,
    metadata_cache: &Arc<MokaFileMetadataCache>,
    metadata_size_hint: Option<usize>,
    concurrency: usize,
) -> Result<Vec<FileGroup>> {
    futures::stream::iter(file_groups)
        .map(|group| {
            let store = Arc::clone(store);
            let cache = Arc::clone(metadata_cache);
            async move {
                let statistics = group.file_statistics(None).cloned().map(Arc::new);
                let mut files = group.into_inner();
                for file in &mut files {
                    let metadata = DFParquetMetadata::new(store.as_ref(), &file.object_meta)
                        .with_metadata_size_hint(file.metadata_size_hint.or(metadata_size_hint))
                        .with_file_metadata_cache(Some(Arc::new(cache.for_file(&file.object_meta))))
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
                }
                let mut group = FileGroup::new(files);
                if let Some(statistics) = statistics {
                    group = group.with_statistics(statistics);
                }
                Ok::<_, DataFusionError>(group)
            }
        })
        .buffered(concurrency)
        .try_collect()
        .await
}
