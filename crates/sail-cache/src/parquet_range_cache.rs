// SPDX-License-Identifier: Apache-2.0

use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::{Arc, Weak};

use bytes::{Bytes, BytesMut};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    CachedParquetFileReaderFactory, ParquetFileReaderFactory,
};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Label, MetricBuilder, MetricType,
};
use datafusion::prelude::SessionConfig;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt};
use quick_cache::sync::Cache;
use quick_cache::{OptionsBuilder, Weighter};

const BLOCK_SIZE: u64 = 512 * 1024;

#[derive(Debug)]
struct StoreId(Weak<dyn ObjectStore>);

impl PartialEq for StoreId {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}
impl Eq for StoreId {}
impl Hash for StoreId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ptr().cast::<()>().hash(state);
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct FileKey {
    store: StoreId,
    path: Path,
    size: u64,
    etag: Option<String>,
    version: Option<String>,
}

impl FileKey {
    fn from_meta(store: &Arc<dyn ObjectStore>, meta: ObjectMeta) -> Option<Self> {
        let etag = meta.e_tag.filter(|s| !s.is_empty() && !s.starts_with("W/"));
        let version = meta.version.filter(|s| !s.is_empty() && s != "null");
        (etag.is_some() || version.is_some()).then(|| Self {
            store: StoreId(Arc::downgrade(store)),
            path: meta.location,
            size: meta.size,
            etag,
            version,
        })
    }
    async fn fetch(
        &self,
        store: &dyn ObjectStore,
        range: Range<u64>,
        scanned: &Count,
    ) -> Result<Bytes> {
        let opts = GetOptions::new()
            .with_range(Some(range.clone()))
            .with_version(self.version.clone())
            .with_if_match(self.etag.clone());
        let response = store.get_opts(&self.path, opts).await?;
        if response.range != range
            || response.meta.size != self.size
            || self
                .etag
                .as_ref()
                .is_some_and(|tag| response.meta.e_tag.as_ref() != Some(tag))
            || self
                .version
                .as_ref()
                .is_some_and(|version| response.meta.version.as_ref() != Some(version))
        {
            return Err(ParquetError::General(format!(
                "Object changed while reading {} at {range:?}",
                self.path
            )));
        }
        let bytes = response.bytes().await?;
        if bytes.len() as u64 != range.end - range.start {
            return Err(ParquetError::EOF(format!(
                "Short range read for {} at {range:?}",
                self.path
            )));
        }
        scanned.add(bytes.len());
        Ok(bytes)
    }
}

type BlockKey = (Arc<FileKey>, u64);

#[derive(Clone)]
struct BlockWeight;

impl Weighter<BlockKey, Bytes> for BlockWeight {
    fn weight(&self, _: &BlockKey, bytes: &Bytes) -> u64 {
        bytes.len() as u64
    }
}

#[derive(Debug)]
pub struct ParquetRangeCache {
    block_size: u64,
    blocks: Cache<BlockKey, Bytes, BlockWeight>,
}

impl ParquetRangeCache {
    pub fn configure(config: &mut SessionConfig, capacity: usize) -> datafusion_common::Result<()> {
        if capacity > 0 {
            config.set_extension(Arc::new(Self::new(capacity)?));
        }
        Ok(())
    }

    fn new(capacity: usize) -> datafusion_common::Result<Self> {
        // A single shard enforces the exact byte limit even for a partial final block.
        let mut options = OptionsBuilder::new();
        if capacity as u64 <= 2 * BLOCK_SIZE {
            // Admit a full block even when it occupies the entire cache.
            options.hot_allocation(1.0);
        }
        let options = options
            .shards(1)
            .weight_capacity(capacity as u64)
            .estimated_items_capacity((capacity / BLOCK_SIZE as usize).clamp(1, 65536))
            .build()
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            block_size: BLOCK_SIZE.min(capacity as u64),
            blocks: Cache::with_options(
                options,
                BlockWeight,
                Default::default(),
                Default::default(),
            ),
        })
    }

    async fn read(
        &self,
        store: &dyn ObjectStore,
        file: &Arc<FileKey>,
        range: Range<u64>,
        scanned: &Count,
    ) -> Result<Bytes> {
        let range = GetRange::Bounded(range)
            .as_range(file.size)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        // A tiny cache must not turn a large scan into thousands of tiny requests.
        if self.block_size < BLOCK_SIZE && range.end - range.start > self.block_size {
            return file.fetch(store, range, scanned).await;
        }
        let mut joined = BytesMut::new();
        let mut pos = range.start;
        while pos < range.end {
            let start = pos / self.block_size * self.block_size;
            let end = start.saturating_add(self.block_size).min(file.size);
            let block = self
                .blocks
                .get_or_insert_async(&(Arc::clone(file), start), async {
                    let bytes = file.fetch(store, start..end, scanned).await?;
                    // A store can return a slice backed by a much larger allocation.
                    Ok::<_, ParquetError>(Bytes::copy_from_slice(&bytes))
                })
                .await?;
            let next = end.min(range.end);
            let slice = block.slice((pos - start) as usize..(next - start) as usize);
            if pos == range.start {
                if next == range.end {
                    return Ok(slice);
                }
                joined.reserve(
                    usize::try_from(range.end - range.start)
                        .map_err(|e| ParquetError::External(Box::new(e)))?,
                );
            }
            joined.extend_from_slice(&slice);
            pos = next;
        }
        Ok(joined.freeze())
    }
}

pub fn parquet_reader_factory(
    config: &SessionConfig,
    store: Arc<dyn ObjectStore>,
    metadata: Arc<FileMetadataCache>,
) -> Arc<dyn ParquetFileReaderFactory> {
    let inner = CachedParquetFileReaderFactory::new(Arc::clone(&store), metadata);
    match config.get_extension::<ParquetRangeCache>() {
        Some(cache) => Arc::new(RangeReaderFactory {
            store,
            inner,
            cache,
        }),
        None => Arc::new(inner),
    }
}

#[derive(Debug)]
struct RangeReaderFactory {
    store: Arc<dyn ObjectStore>,
    inner: CachedParquetFileReaderFactory,
    cache: Arc<ParquetRangeCache>,
}

impl ParquetFileReaderFactory for RangeReaderFactory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let path = file.object_meta.location.clone();
        let scanned = MetricBuilder::new(metrics)
            .with_label(Label::new("filename", path.to_string()))
            .with_type(MetricType::Summary)
            .bytes_counter("bytes_scanned", partition);
        Ok(Box::new(RangeReader {
            inner: self.inner.create_reader(partition, file, hint, metrics)?,
            store: Arc::clone(&self.store),
            cache: Arc::clone(&self.cache),
            path,
            identity: None,
            scanned,
        }))
    }
}

struct RangeReader {
    inner: Box<dyn AsyncFileReader + Send>,
    store: Arc<dyn ObjectStore>,
    cache: Arc<ParquetRangeCache>,
    path: Path,
    // Resolved once on the first data read. Some manifests only supply the file size.
    identity: Option<Option<Arc<FileKey>>>,
    scanned: Count,
}

impl RangeReader {
    async fn file_key(&mut self) -> Result<Option<Arc<FileKey>>> {
        if let Some(key) = &self.identity {
            return Ok(key.clone());
        }
        let meta = self.store.head(&self.path).await?;
        let key = FileKey::from_meta(&self.store, meta).map(Arc::new);
        self.identity = Some(key.clone());
        Ok(key)
    }
}

impl AsyncFileReader for RangeReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            match self.file_key().await? {
                Some(key) => {
                    self.cache
                        .read(self.store.as_ref(), &key, range, &self.scanned)
                        .await
                }
                None => self.inner.get_bytes(range).await,
            }
        })
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        Box::pin(async move {
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            let Some(key) = self.file_key().await? else {
                return self.inner.get_byte_ranges(ranges).await;
            };
            stream::iter(ranges)
                .map(|range| {
                    self.cache
                        .read(self.store.as_ref(), &key, range, &self.scanned)
                })
                .buffered(10)
                .try_collect()
                .await
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};
    use object_store::memory::InMemory;

    use super::*;

    fn factory(
        config: &SessionConfig,
        store: Arc<dyn ObjectStore>,
    ) -> datafusion_common::Result<Arc<dyn ParquetFileReaderFactory>> {
        let metadata =
            CacheManager::try_new(&CacheManagerConfig::default())?.get_file_metadata_cache();
        Ok(parquet_reader_factory(config, store, metadata))
    }

    fn reader(
        factory: &dyn ParquetFileReaderFactory,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        factory.create_reader(0, PartitionedFile::new("table/test", 12), None, metrics)
    }

    #[tokio::test]
    async fn readers_share_overlapping_ranges_and_separate_object_generations()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        ParquetRangeCache::configure(&mut config, 8)?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("table/test");
        store.put(&path, "abcdefghijkl".into()).await?;
        let a = factory(&config, Arc::clone(&store))?;
        let b = factory(&config, Arc::clone(&store))?;
        let metrics = ExecutionPlanMetricsSet::new();
        let mut r1 = reader(a.as_ref(), &metrics)?;
        let mut r2 = reader(b.as_ref(), &metrics)?;
        let (p, q) = futures::try_join!(r1.get_bytes(2..5), r2.get_bytes(0..2))?;
        assert_eq!(&p[..], b"cde");
        assert_eq!(&q[..], b"ab");
        assert_eq!(
            metrics
                .clone_inner()
                .sum_by_name("bytes_scanned")
                .map(|v| v.as_usize()),
            Some(8)
        );
        let ranges = r2.get_byte_ranges(vec![7..14, 8..10, 6..9]).await?;
        assert_eq!(
            ranges,
            vec![
                Bytes::from_static(b"hijkl"),
                Bytes::from_static(b"ij"),
                Bytes::from_static(b"ghi")
            ]
        );
        let cache = config
            .get_extension::<ParquetRangeCache>()
            .ok_or("missing cache")?;
        assert!(cache.blocks.weight() <= 8);
        assert_eq!(cache.blocks.len(), 1);

        store.put(&path, "ABCDEFGHIJKL".into()).await?;
        let mut changed = reader(a.as_ref(), &metrics)?;
        assert_eq!(&changed.get_bytes(3..6).await?[..], b"DEF");
        assert!(r1.get_bytes(8..12).await.is_err());
        store.delete(&path).await?;
        assert!(reader(a.as_ref(), &metrics)?.get_bytes(3..6).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn identical_paths_and_etags_in_distinct_stores_are_isolated()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        ParquetRangeCache::configure(&mut config, 0)?;
        assert!(config.get_extension::<ParquetRangeCache>().is_none());
        ParquetRangeCache::configure(&mut config, 64)?;
        let path = Path::from("table/test");
        for value in ["abcdefghijkl", "ABCDEFGHIJKL"] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            store.put(&path, value.into()).await?;
            let mut meta = store.head(&path).await?;
            meta.e_tag = Some("W/\"weak\"".into());
            meta.version = Some("null".into());
            assert!(FileKey::from_meta(&store, meta).is_none());
            let factory = factory(&config, store)?;
            let mut reader = reader(factory.as_ref(), &ExecutionPlanMetricsSet::new())?;
            assert_eq!(&reader.get_bytes(0..5).await?[..], &value.as_bytes()[..5]);
        }
        Ok(())
    }
}
