mod cache_read;
mod cache_tests;
mod cache_write;
mod shuffle_read;
mod shuffle_write;
mod stage_input;

use std::fmt::Display;
use std::sync::Arc;

pub use cache_read::CacheReadExec;
pub(crate) use cache_write::CacheWriteExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::cache_manager::CacheId;
use sail_server::actor::{Actor, ActorHandle};
pub(crate) use shuffle_read::ShuffleReadExec;
pub(crate) use shuffle_write::ShuffleWriteExec;
pub(crate) use stage_input::StageInputExec;

use crate::local_cache_store::LocalCacheStore;

/// Reports cache partition materialization back to the runtime.
pub(crate) trait CachePartitionReporter: Send + Sync {
    fn report_partition_stored(&self, cache_id: CacheId, partition: usize);
}

/// Builds a cache-partition-stored message for an actor.
pub(crate) trait CachePartitionReporterMessage {
    fn cache_partition_stored(cache_id: CacheId, partition: usize) -> Self;
}

/// Reports cache partition materialization by sending an actor message.
pub(crate) struct ActorCachePartitionReporter<T: Actor> {
    handle: ActorHandle<T>,
}

impl<T: Actor> ActorCachePartitionReporter<T> {
    /// Creates a reporter backed by the given actor handle.
    pub fn new(handle: ActorHandle<T>) -> Self {
        Self { handle }
    }
}

impl<T: Actor> CachePartitionReporter for ActorCachePartitionReporter<T>
where
    T::Message: CachePartitionReporterMessage,
{
    fn report_partition_stored(&self, cache_id: CacheId, partition: usize) {
        let handle = self.handle.clone();
        tokio::spawn(async move {
            let _ = handle
                .send(T::Message::cache_partition_stored(cache_id, partition))
                .await;
        });
    }
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

/// Injects a cache partition reporter into all cache write nodes in a physical plan.
pub(crate) fn inject_cache_write_reporter(
    plan: Arc<dyn ExecutionPlan>,
    reporter: Arc<dyn CachePartitionReporter>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|node| {
        if let Some(cache_write) = node.as_any().downcast_ref::<CacheWriteExec>() {
            let mut write = cache_write.clone();
            write.set_cache_reporter(reporter.clone());
            Ok(Transformed::yes(Arc::new(write)))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}

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
