// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::common::Result;

use super::context::PlannerContext;
pub use crate::kernel::log_segment::{
    list_log_segment_files as kernel_list_log_segment_files,
    resolve_log_segment_files as kernel_resolve_log_segment_files, LogSegmentFiles,
    LogSegmentResolveOptions,
};

/// List Delta log files up to `max_version`, using the planner-local cache when available.
pub async fn list_log_segment_files(
    ctx: &PlannerContext<'_>,
    max_version: i64,
) -> Result<LogSegmentFiles> {
    if let Some(files) = ctx.get_cached_log_segment_files(max_version) {
        return Ok(files);
    }
    let log_store = ctx.log_store()?;
    let files = kernel_list_log_segment_files(&log_store, max_version)
        .await
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
    ctx.set_cached_log_segment_files(max_version, files.clone());
    Ok(files)
}

/// Resolve the minimal set of Delta log files needed to replay state up to `max_version`,
/// using the planner-local cache for the initial listing and applying `options` on top.
pub async fn resolve_log_segment_files(
    ctx: &PlannerContext<'_>,
    max_version: i64,
    options: LogSegmentResolveOptions,
) -> Result<LogSegmentFiles> {
    // Obtain the full listing (possibly from cache), then apply resolve options.
    let cached = list_log_segment_files(ctx, max_version).await?;

    // Re-apply the resolve logic on the cached listing.
    let log_store = ctx.log_store()?;
    kernel_resolve_log_segment_files(&log_store, max_version, options)
        .await
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
        .map(|mut resolved| {
            // Preserve checkpoint files from the cached listing to avoid a second store round-trip.
            resolved.checkpoint_files = cached.checkpoint_files;
            resolved
        })
}
