/// LRU cache for prepared statements with size-based eviction
///
/// This module implements a cache that:
/// - Stores prepared statement schemas and execution status
/// - Uses LRU (Least Recently Used) eviction policy
/// - Enforces a maximum memory limit
/// - Tracks cache statistics (hits/misses/evictions)
use std::sync::Arc;

use arrow::datatypes::Schema;
use log::{debug, info, warn};
use lru::LruCache;
use tokio::sync::RwLock;

/// Entry stored in the prepared statement cache
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Schema of the query result
    pub schema: Arc<Schema>,
    /// Whether the query was already executed (true for DDL)
    pub was_executed: bool,
    /// Estimated size in bytes (schema + metadata)
    pub size_bytes: usize,
}

impl CacheEntry {
    /// Estimated overhead per field for Arc, metadata, and other internal structures
    const FIELD_OVERHEAD_BYTES: usize = 64;

    pub fn new(schema: Arc<Schema>, was_executed: bool) -> Self {
        let size_bytes = Self::estimate_schema_size(&schema);
        Self {
            schema,
            was_executed,
            size_bytes,
        }
    }

    /// Estimate memory usage of a schema (rough approximation)
    fn estimate_schema_size(schema: &Schema) -> usize {
        // Base overhead
        let mut size = std::mem::size_of::<Schema>();

        // Field names and types
        for field in schema.fields() {
            size += field.name().len();
            size += std::mem::size_of_val(field.data_type());
            // Add overhead for Arc, metadata, and other internal structures
            size += Self::FIELD_OVERHEAD_BYTES;
        }

        // Metadata
        for (key, value) in schema.metadata() {
            size += key.len() + value.len();
        }

        size
    }
}

/// Statistics for cache monitoring
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_size_bytes: usize,
    pub current_entries: usize,
}

/// LRU cache for prepared statements
pub struct PreparedStatementCache {
    cache: RwLock<LruCache<String, CacheEntry>>,
    max_size_bytes: usize,
    current_size_bytes: RwLock<usize>,
    stats: RwLock<CacheStats>,
    enable_stats: bool,
}

impl PreparedStatementCache {
    /// Create a new cache with the specified maximum size
    ///
    /// # Arguments
    ///
    /// * `max_size_bytes` - Maximum total size in bytes (e.g., 1 GB)
    /// * `enable_stats` - Enable statistics tracking
    ///
    /// # Note
    ///
    /// The cache uses an unbounded LRU internally but enforces size limits
    /// by evicting entries when the total size exceeds `max_size_bytes`.
    pub fn new(max_size_bytes: usize, enable_stats: bool) -> Self {
        info!(
            "Initializing PreparedStatementCache: max_size = {} MB, stats = {}",
            max_size_bytes / (1024 * 1024),
            enable_stats
        );

        Self {
            // Use a large capacity - we'll manage size ourselves
            cache: RwLock::new(LruCache::unbounded()),
            max_size_bytes,
            current_size_bytes: RwLock::new(0),
            stats: RwLock::new(CacheStats::default()),
            enable_stats,
        }
    }

    /// Get an entry from the cache
    pub async fn get(&self, sql: &str) -> Option<CacheEntry> {
        let mut cache = self.cache.write().await;
        let result = cache.get(sql).cloned();

        if self.enable_stats {
            let mut stats = self.stats.write().await;
            if result.is_some() {
                stats.hits += 1;
                debug!(
                    "CACHE HIT: {} (hits={}, misses={})",
                    sql, stats.hits, stats.misses
                );
            } else {
                stats.misses += 1;
                debug!(
                    "CACHE MISS: {} (hits={}, misses={})",
                    sql, stats.hits, stats.misses
                );
            }
        }

        result
    }

    /// Insert an entry into the cache, evicting LRU entries if needed
    pub async fn insert(&self, sql: String, entry: CacheEntry) {
        let entry_size = entry.size_bytes;

        // Evict entries if we would exceed the size limit
        self.evict_to_fit(entry_size).await;

        // Insert the new entry
        let mut cache = self.cache.write().await;
        let mut current_size = self.current_size_bytes.write().await;

        // If there was an old entry, subtract its size
        if let Some(old_entry) = cache.put(sql.clone(), entry) {
            *current_size = current_size.saturating_sub(old_entry.size_bytes);
        }

        // Add the new entry's size
        *current_size += entry_size;

        if self.enable_stats {
            let mut stats = self.stats.write().await;
            stats.current_size_bytes = *current_size;
            stats.current_entries = cache.len();

            let usage_pct = (*current_size as f64 / self.max_size_bytes as f64) * 100.0;
            info!(
                "CACHED: {} (size: {:.2} KB, cache: {:.2}/{} MB [{:.1}%], entries: {})",
                sql,
                entry_size as f64 / 1024.0,
                *current_size as f64 / (1024.0 * 1024.0),
                self.max_size_bytes / (1024 * 1024),
                usage_pct,
                cache.len()
            );
        }
    }

    /// Evict LRU entries until we have room for `needed_bytes`
    async fn evict_to_fit(&self, needed_bytes: usize) {
        let mut cache = self.cache.write().await;
        let mut current_size = self.current_size_bytes.write().await;

        let mut evictions = 0;
        let mut freed_bytes = 0;

        while *current_size + needed_bytes > self.max_size_bytes {
            if let Some((sql, entry)) = cache.pop_lru() {
                *current_size = current_size.saturating_sub(entry.size_bytes);
                freed_bytes += entry.size_bytes;
                evictions += 1;

                if self.enable_stats {
                    warn!(
                        "EVICTED (LRU): {} (freed: {:.2} KB, cache: {:.2}/{} MB)",
                        sql,
                        entry.size_bytes as f64 / 1024.0,
                        *current_size as f64 / (1024.0 * 1024.0),
                        self.max_size_bytes / (1024 * 1024)
                    );
                }
            } else {
                // Cache is empty but still not enough room - shouldn't happen
                warn!(
                    "Cannot fit entry of {:.2} KB (max: {} MB)",
                    needed_bytes as f64 / 1024.0,
                    self.max_size_bytes / (1024 * 1024)
                );
                break;
            }
        }

        if self.enable_stats && evictions > 0 {
            let mut stats = self.stats.write().await;
            stats.evictions += evictions;
            info!(
                "Eviction summary: {} entries removed, {:.2} MB freed",
                evictions,
                freed_bytes as f64 / (1024.0 * 1024.0)
            );
        }
    }

    /// Get cache statistics
    #[allow(dead_code)]
    pub async fn stats(&self) -> CacheStats {
        if !self.enable_stats {
            return CacheStats::default();
        }

        let stats = self.stats.read().await;
        let current_size = self.current_size_bytes.read().await;
        let cache = self.cache.read().await;

        CacheStats {
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            current_size_bytes: *current_size,
            current_entries: cache.len(),
        }
    }

    /// Clear all entries from the cache
    #[allow(dead_code)]
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        let mut current_size = self.current_size_bytes.write().await;

        let entries = cache.len();
        cache.clear();
        *current_size = 0;

        if self.enable_stats {
            let mut stats = self.stats.write().await;
            stats.current_size_bytes = 0;
            stats.current_entries = 0;
        }

        info!("CACHE CLEARED: {} entries removed", entries);
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let cache = PreparedStatementCache::new(1024 * 1024, true); // 1 MB
        let schema = create_test_schema();

        // Miss on first access
        assert!(cache.get("SELECT 1").await.is_none());

        // Insert and hit
        let entry = CacheEntry::new(schema.clone(), false);
        cache.insert("SELECT 1".to_string(), entry).await;
        assert!(cache.get("SELECT 1").await.is_some());

        // Stats
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.current_entries, 1);
    }

    #[tokio::test]
    async fn test_cache_lru_eviction() {
        // Small cache that can only hold ~2 entries
        let cache = PreparedStatementCache::new(500, true);
        let schema = create_test_schema();

        // Insert 3 entries
        cache
            .insert("SQL1".to_string(), CacheEntry::new(schema.clone(), false))
            .await;
        cache
            .insert("SQL2".to_string(), CacheEntry::new(schema.clone(), false))
            .await;
        cache
            .insert("SQL3".to_string(), CacheEntry::new(schema.clone(), false))
            .await;

        // First entry should have been evicted
        let stats = cache.stats().await;
        assert!(stats.evictions > 0);

        // SQL1 (oldest) should be evicted
        assert!(cache.get("SQL1").await.is_none());
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = PreparedStatementCache::new(1024 * 1024, true);
        let schema = create_test_schema();

        cache
            .insert("SQL1".to_string(), CacheEntry::new(schema.clone(), false))
            .await;
        cache
            .insert("SQL2".to_string(), CacheEntry::new(schema, false))
            .await;

        let stats_before = cache.stats().await;
        assert_eq!(stats_before.current_entries, 2);

        cache.clear().await;

        let stats_after = cache.stats().await;
        assert_eq!(stats_after.current_entries, 0);
        assert_eq!(stats_after.current_size_bytes, 0);
    }
}
