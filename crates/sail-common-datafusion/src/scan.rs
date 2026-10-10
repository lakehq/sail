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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::object_store::{
        self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use datafusion::parquet::data_type::Int64Type;
    use datafusion::parquet::file::writer::SerializedFileWriter;
    use datafusion::parquet::schema::parser::parse_message_type;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ColumnStatistics, Statistics};
    use futures::stream::BoxStream;
    use tokio::sync::Notify;

    use super::*;

    #[derive(Debug)]
    struct ControlledStore {
        inner: InMemory,
        gates: HashMap<Path, Notify>,
        requests: AtomicUsize,
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    impl std::fmt::Display for ControlledStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ControlledStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ControlledStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.gates[location].notified().await;
            let result = self.inner.get_opts(location, options).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            result
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn metadata_loads_are_concurrent_and_preserve_groups() -> Result<()> {
        let schema = Arc::new(parse_message_type("message test { REQUIRED INT64 id; }")?);
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, Default::default())?;
        let mut row_group = writer.next_row_group()?;
        let mut column = row_group.next_column()?.expect("id column");
        column.typed::<Int64Type>().write_batch(&[1, 2, 3], None, None)?;
        column.close()?;
        row_group.close()?;
        let data = writer.into_inner()?;
        let metadata_size_hint = data.len();
        let payload = PutPayload::from(data);

        let inner = InMemory::new();
        let paths = ["a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet"]
            .map(Path::from);
        let mut files = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            inner.put(path, payload.clone()).await?;
            let mut file = PartitionedFile::from(inner.head(path).await?);
            file.metadata_size_hint = Some(metadata_size_hint);
            file.extensions.insert(index);
            files.push(file);
        }
        let statistics = [9, 0, 6].map(|num_rows| {
            Arc::new(Statistics {
                num_rows: Precision::Exact(num_rows),
                total_byte_size: Precision::Exact(num_rows * 8),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            })
        });
        let remaining = files.split_off(3);
        let groups = vec![
            FileGroup::new(files).with_statistics(Arc::clone(&statistics[0])),
            FileGroup::default().with_statistics(Arc::clone(&statistics[1])),
            FileGroup::new(remaining).with_statistics(Arc::clone(&statistics[2])),
        ];
        let controlled = Arc::new(ControlledStore {
            inner,
            gates: paths.iter().map(|path| (path.clone(), Notify::new())).collect(),
            requests: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let store = Arc::clone(&controlled) as Arc<dyn ObjectStore>;
        let cache = CacheManager::try_new(&CacheManagerConfig::default())?
            .get_file_metadata_cache();
        assert!(paths.iter().all(|path| cache.get(path).is_none()));

        let load = load_parquet_scan_metadata(groups, &store, &cache, Some(8), 2);
        futures::pin_mut!(load);
        assert!(futures::poll!(&mut load).is_pending());
        assert_eq!(controlled.active.load(Ordering::SeqCst), 2);
        assert_eq!(controlled.requests.load(Ordering::SeqCst), 2);

        // Hold the first file until last so completion order differs from file order.
        for (index, path) in paths.iter().enumerate().skip(1) {
            controlled.gates[path].notify_one();
            assert!(futures::poll!(&mut load).is_pending());
            assert_eq!(controlled.peak.load(Ordering::SeqCst), 2);
            assert_eq!(
                controlled.requests.load(Ordering::SeqCst),
                (index + 2).min(paths.len()),
            );
        }
        controlled.gates[&paths[0]].notify_one();
        let Poll::Ready(result) = futures::poll!(&mut load) else {
            panic!("all released footer loads must complete");
        };
        let groups = result?;
        assert_eq!(controlled.active.load(Ordering::SeqCst), 0);
        assert_eq!(controlled.peak.load(Ordering::SeqCst), 2);
        assert_eq!(controlled.requests.load(Ordering::SeqCst), paths.len());
        assert_eq!(groups.iter().map(FileGroup::len).collect::<Vec<_>>(), [3, 0, 2]);
        for (group, statistics) in groups.iter().zip(&statistics) {
            assert_eq!(group.file_statistics(None), Some(statistics.as_ref()));
        }
        for (index, file) in groups.iter().flat_map(FileGroup::iter).enumerate() {
            assert_eq!(file.path(), &paths[index]);
            assert_eq!(file.metadata_size_hint, Some(metadata_size_hint));
            assert_eq!(file.extensions.get::<usize>(), Some(&index));
            let metadata = file.extensions.get::<ParquetScanMetadata>().expect("row groups");
            assert_eq!(metadata.row_groups.len(), 1);
            assert_eq!(metadata.row_groups[0].num_rows, 3);
            assert!(cache.get(file.path()).is_some());
        }
        Ok(())
    }
}
