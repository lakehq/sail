mod cache_read;
mod cache_tests;
mod cache_write;
mod shuffle_read;
mod shuffle_write;
mod stage_input;

use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::local_cache_store::LocalCacheStore;
pub use cache_read::CacheReadExec;
pub(crate) use cache_write::CacheWriteExec;
pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::ShuffleWriteExec;
pub(crate) use stage_input::StageInputExec;

/// Notifies the runtime when a cache partition is stored locally.
pub(crate) trait CachePartitionNotifier: Send + Sync {
    fn notify(&self, cache_id: u64, partition: usize);
}

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

/// Injects a cache partition notifier into all cache write nodes in a physical plan.
pub(crate) fn inject_cache_write_notifier(
    plan: Arc<dyn ExecutionPlan>,
    cache_notifier: Arc<dyn CachePartitionNotifier>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        if let Some(cache_write) = node.as_any().downcast_ref::<CacheWriteExec>() {
            let mut write = cache_write.clone();
            write.set_cache_notifier(cache_notifier.clone());
            Ok(Transformed::yes(Arc::new(write)))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}

#[allow(clippy::all)]
pub(crate) mod gen {
    tonic::include_proto!("sail.plan");
}

/// The way in which a shuffle stream is consumed by downstream tasks.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShuffleConsumption {
    /// Each shuffle stream is consumed by a single downstream tasks.
    Single,
    /// Each shuffle stream is consumed by multiple downstream tasks.
    Multiple,
}

struct ListListDisplay<'a, T: Display>(pub &'a [Vec<T>]);

impl<'a, T: Display> Display for ListListDisplay<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, list) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "[")?;
            for (j, item) in list.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{item}")?;
            }
            write!(f, "]")?;
        }
        write!(f, "]")
    }
}
