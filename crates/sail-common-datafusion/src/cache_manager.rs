use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use datafusion::logical_expr::LogicalPlan;

use crate::extension::SessionExtension;

/// A cached plan entry.
#[derive(Clone)]
pub struct CachedData {
    /// The resolved logical plan used as the cache key.
    pub plan: LogicalPlan,
    /// Unique identifier for this cache entry.
    pub cache_id: String,
    /// Whether the cached data has been materialized on worker nodes.
    pub materialized: bool,
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
    pub fn cache_plan(&self, plan: LogicalPlan) -> String {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = entries.iter().find(|e| e.plan == plan) {
            return existing.cache_id.clone();
        }
        let cache_id = format!("cache_{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        entries.push(CachedData {
            plan,
            cache_id: cache_id.clone(),
            materialized: false,
        });
        cache_id
    }

    /// Returns a clone of the cached entry if the given plan matches.
    pub fn find_match(&self, node: &LogicalPlan) -> Option<CachedData> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.iter().find(|e| e.plan == *node).map(|e| e.clone())
    }

    /// Returns a clone of the cached entry with the given cache ID.
    pub fn find_by_id(&self, cache_id: &str) -> Option<CachedData> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .find(|e| e.cache_id == cache_id)
            .map(|e| e.clone())
    }

    /// Marks a cache entry as materialized.
    pub fn mark_materialized(&self, cache_id: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.iter_mut().find(|e| e.cache_id == cache_id) {
            entry.materialized = true;
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}
