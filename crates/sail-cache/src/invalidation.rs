use std::task::Poll;

use datafusion::common::Result;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::path::Path;

use crate::file_caches::FileCaches;
use crate::file_listing_cache::MokaFileListingCache;

/// Invalidate listings on the driver, including when a write runs on remote workers.
pub struct FileCacheInvalidation {
    targets: Vec<(MokaFileListingCache, Path)>,
}

impl FileCacheInvalidation {
    pub fn new(ctx: &SessionContext, paths: &[ListingTableUrl]) -> Result<Self> {
        let mut targets = Vec::new();
        if paths.is_empty() {
            return Ok(Self { targets });
        }
        let state = ctx.state_ref();
        let caches = FileCaches::from_config(state.read().config());
        if let Some(cache) = &caches.listing {
            for path in paths {
                let store = ctx.runtime_env().object_store(path)?;
                targets.push((cache.for_store(&store), path.prefix().clone()));
            }
        }
        let guard = Self { targets };
        guard.invalidate();
        Ok(guard)
    }

    pub fn wrap(self, mut stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        if self.targets.is_empty() {
            return stream;
        }
        let schema = stream.schema();
        let mut guard = Some(self);
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::poll_fn(move |cx| {
                let result = stream.poll_next_unpin(cx);
                if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
                    guard.take();
                }
                result
            }),
        ))
    }

    fn invalidate(&self) {
        for (cache, prefix) in &self.targets {
            cache.invalidate(|key| {
                key.path.prefix_match(prefix).is_some() || prefix.prefix_match(&key.path).is_some()
            });
        }
    }
}

impl Drop for FileCacheInvalidation {
    fn drop(&mut self) {
        self.invalidate();
    }
}
