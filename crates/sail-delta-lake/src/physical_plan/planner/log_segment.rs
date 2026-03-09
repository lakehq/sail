use datafusion::common::Result;

use super::context::PlannerContext;
pub use crate::kernel::log_segment::{
    list_log_segment_files as kernel_list_log_segment_files, LogSegmentFiles,
    LogSegmentResolveOptions,
};
use crate::spec::{parse_commit_version, parse_version_prefix};

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
    let mut files = list_log_segment_files(ctx, max_version).await?;

    // Avoid double-counting actions already materialized into the latest checkpoint:
    // only replay commit JSONs strictly newer than that checkpoint version.
    let latest_checkpoint_version = files
        .checkpoint_files
        .iter()
        .filter_map(|f| parse_version_prefix(f))
        .max();
    if let Some(cp_ver) = latest_checkpoint_version {
        files
            .commit_files
            .retain(|f| parse_commit_version(f).map(|v| v > cp_ver).unwrap_or(true));
    }

    if let Some((start, end)) = options.commit_version_range {
        files.commit_files.retain(|f| {
            parse_commit_version(f)
                .map(|v| v >= start && v <= end)
                .unwrap_or(false)
        });
    }

    Ok(files)
}
