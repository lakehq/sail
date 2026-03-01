use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use datafusion::logical_expr::LogicalPlan;

use crate::extension::SessionExtension;

/// Strongly typed identifier for a cached plan entry.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct CacheId(u64);

impl From<u64> for CacheId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<CacheId> for u64 {
    fn from(value: CacheId) -> Self {
        value.0
    }
}

impl fmt::Display for CacheId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A cached plan entry.
#[derive(Clone)]
pub struct CachedData {
    /// The resolved logical plan used as the cache key.
    pub plan: LogicalPlan,
    /// Unique identifier for this cache entry.
    pub cache_id: CacheId,
    /// Whether the cached data has been materialized on worker nodes.
    pub materialized: bool,
    /// Number of partitions produced when this cache entry was materialized.
    pub num_partitions: Option<usize>,
}

/// Manages cached query results for a session.
pub struct CacheManager {
    entries: Mutex<Vec<CachedData>>,
    next_id: AtomicU64,
}

impl SessionExtension for CacheManager {
    fn name() -> &'static str {
        "cache manager"
    }
}

impl CacheManager {
    /// Creates a new empty cache manager.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Registers a plan for caching. Returns the assigned cache ID.
    pub fn cache_plan(&self, plan: LogicalPlan) -> CacheId {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = entries.iter().find(|e| e.plan == plan) {
            return existing.cache_id;
        }
        let cache_id = CacheId::from(self.next_id.fetch_add(1, Ordering::Relaxed));
        entries.push(CachedData {
            plan,
            cache_id,
            materialized: false,
            num_partitions: None,
        });
        cache_id
    }

    /// Returns a clone of the cached entry if the given plan matches.
    pub fn find_match(&self, node: &LogicalPlan) -> Option<CachedData> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.iter().find(|e| e.plan == *node).cloned()
    }

    /// Returns a clone of the cached entry with the given cache ID.
    pub fn find_by_id(&self, cache_id: CacheId) -> Option<CachedData> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.iter().find(|e| e.cache_id == cache_id).cloned()
    }

    /// Marks a cache entry as materialized with the given partition count.
    pub fn mark_materialized(&self, cache_id: CacheId, num_partitions: usize) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.iter_mut().find(|e| e.cache_id == cache_id) {
            entry.materialized = true;
            entry.num_partitions = Some(num_partitions);
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}
