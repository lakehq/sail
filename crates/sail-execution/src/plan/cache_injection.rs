use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::local_cache_store::LocalCacheStore;
use crate::plan::{CacheReadExec, CacheWriteExec};

/// Injects a worker-local [`LocalCacheStore`] into all cache exec nodes in a physical plan.
///
/// `CacheReadExec` / `CacheWriteExec` are constructed as "stubs" (without the store) during
/// planning/serialization; this wiring step attaches the concrete store on the execution side.
pub(crate) fn inject_local_cache_store(
    plan: Arc<dyn ExecutionPlan>,
    cache_store: Arc<LocalCacheStore>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        if let Some(cache_read) = node.as_any().downcast_ref::<CacheReadExec>() {
            let mut read = cache_read.clone();
            read.set_cache_store(cache_store.clone());
            Ok(Transformed::yes(Arc::new(read)))
        } else if let Some(cache_write) = node.as_any().downcast_ref::<CacheWriteExec>() {
            let mut write = cache_write.clone();
            write.set_cache_store(cache_store.clone());
            Ok(Transformed::yes(Arc::new(write)))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}
