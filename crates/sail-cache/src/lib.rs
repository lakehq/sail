use std::sync::Mutex;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use sail_common_datafusion::extension::SessionExtension;

/// A cached plan entry pairing a plan key with its materialized data.
struct CachedData {
    /// The resolved logical plan used as the cache key.
    plan: LogicalPlan,
    /// The materialized result, or None if not yet executed.
    batches: Option<Vec<RecordBatch>>,
}

/// Manages cached query results for a session.
pub struct CacheManager {
    entries: Mutex<Vec<CachedData>>,
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
        }
    }

    /// Registers a plan for caching. Data is not materialized yet.
    pub fn cache_plan(&self, plan: LogicalPlan) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if !entries.iter().any(|e| e.plan == plan) {
            entries.push(CachedData {
                plan,
                batches: None,
            });
        }
    }

    /// Removes a cached plan.
    pub fn uncache_plan(&self, plan: &LogicalPlan) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|e| e.plan != *plan);
    }

    /// Stores materialized batches for a cached plan after first execution.
    pub fn store_batches(&self, plan: &LogicalPlan, batches: Vec<RecordBatch>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.iter_mut().find(|e| e.plan == *plan) {
            entry.batches = Some(batches);
        }
    }

    /// Checks if a plan is registered for caching but not yet materialized.
    pub fn needs_materialization(&self, plan: &LogicalPlan) -> bool {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .any(|e| e.batches.is_none() && e.plan == *plan)
    }

    /// Walks the plan tree and replaces matching subtrees with cached scans.
    pub fn use_cached_data(&self, ctx: &SessionContext, plan: LogicalPlan) -> Result<LogicalPlan> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        plan.transform_down(|node| {
            for entry in entries.iter() {
                if let Some(batches) = &entry.batches {
                    if node == entry.plan {
                        let cached_scan = create_cached_scan(ctx, batches)?;
                        return Ok(Transformed::yes(cached_scan));
                    }
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
    }

    /// Clears all cached data.
    pub fn clear(&self) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.clear();
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates a logical plan that scans from in-memory RecordBatches.
fn create_cached_scan(_ctx: &SessionContext, _batches: &[RecordBatch]) -> Result<LogicalPlan> {
    todo!("create a MemTable-backed LogicalPlan from cached RecordBatches")
}
