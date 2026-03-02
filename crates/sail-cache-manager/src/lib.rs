use std::sync::{Arc, Mutex};

use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{internal_err, Result};
use datafusion_expr::Extension;
use sail_common::cache_id::CacheId;
use sail_common_datafusion::extension::SessionExtension;
use sail_logical_plan::cache_read_relation::CacheReadRelationNode;
use slotmap::SlotMap;

/// A cached plan entry.
#[derive(Clone)]
pub struct CachedData {
    /// The resolved logical plan used as the cache key.
    pub plan: LogicalPlan,
    /// Whether the cached data has been materialized on worker nodes.
    pub materialized: bool,
    /// Number of partitions produced when this cache entry was materialized.
    pub num_partitions: Option<usize>,
}

/// Manages cached query results for a session.
pub struct CacheManager {
    entries: Mutex<SlotMap<CacheId, CachedData>>,
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
            entries: Mutex::new(SlotMap::with_key()),
        }
    }

    /// Registers a plan for caching. Returns the assigned cache ID.
    pub fn cache_plan(&self, plan: LogicalPlan) -> CacheId {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((cache_id, _)) = entries.iter().find(|(_, entry)| entry.plan == plan) {
            return cache_id;
        }
        entries.insert(CachedData {
            plan,
            materialized: false,
            num_partitions: None,
        })
    }

    /// Returns the cache ID and entry if the given plan matches.
    pub fn find_match(&self, node: &LogicalPlan) -> Option<(CacheId, CachedData)> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .find(|(_, entry)| entry.plan == *node)
            .map(|(cache_id, entry)| (cache_id, entry.clone()))
    }

    /// Returns a clone of the cached entry with the given cache ID.
    pub fn find_by_id(&self, cache_id: CacheId) -> Option<CachedData> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.get(cache_id).cloned()
    }

    /// Returns cache entries for all IDs, or errors with the missing IDs.
    pub fn get_required_by_ids(&self, cache_ids: &[CacheId]) -> Result<Vec<(CacheId, CachedData)>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let mut found = Vec::with_capacity(cache_ids.len());
        let mut missing = Vec::new();

        for &cache_id in cache_ids {
            if let Some(entry) = entries.get(cache_id) {
                found.push((cache_id, entry.clone()));
            } else {
                missing.push(cache_id);
            }
        }
        if !missing.is_empty() {
            return internal_err!("missing required cache id(s): {missing:?}");
        }
        Ok(found)
    }

    /// Marks a cache entry as materialized with the given partition count.
    pub fn mark_materialized(&self, cache_id: CacheId, num_partitions: usize) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.get_mut(cache_id) {
            entry.materialized = true;
            entry.num_partitions = Some(num_partitions);
        }
    }

    /// Replaces cached subtrees with CacheReadRelation nodes.
    pub fn rewrite_plan_with_cache_reads(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(|node| {
            let Some((cache_id, cached)) = self.find_match(&node) else {
                return Ok(Transformed::no(node));
            };
            let relation = CacheReadRelationNode::new(cached.plan.schema().clone(), cache_id);
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(relation),
            })))
        })
        .map(|t| t.data)
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}
