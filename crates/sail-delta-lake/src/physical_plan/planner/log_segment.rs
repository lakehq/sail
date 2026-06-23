use std::collections::BTreeMap;

use datafusion::common::Result;

use super::context::PlannerContext;
pub use crate::kernel::log_segment::{
    list_log_segment_files as kernel_list_log_segment_files, LogSegmentFiles,
    LogSegmentResolveOptions,
};
use crate::kernel::{catalog_managed_commit_file_name, CatalogManagedCommitSet};
use crate::spec::{parse_commit_version, parse_compacted_json_versions, parse_version_prefix};

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
    catalog_managed_commits: Option<&CatalogManagedCommitSet>,
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
        files.compaction_files.retain(|f| {
            parse_compacted_json_versions(f)
                .map(|(s, _)| s > cp_ver)
                .unwrap_or(true)
        });
    }

    if let Some((start, end)) = options.commit_version_range {
        files.commit_files.retain(|f| {
            parse_log_file_version(f)
                .map(|v| v >= start && v <= end)
                .unwrap_or(false)
        });
        files.compaction_files.retain(|f| {
            parse_compacted_json_versions(f)
                .map(|(s, e)| s >= start && e <= end)
                .unwrap_or(false)
        });
    }

    merge_catalog_managed_commits_for_planner(&mut files, catalog_managed_commits);

    // Merge compaction files into commit files for the metadata-as-data scan path.
    // Both use the same ndjson format and can be read by the same JSON data source.
    if !files.compaction_files.is_empty() {
        let mut parsed: Vec<((i64, i64), String)> = files
            .compaction_files
            .iter()
            .filter_map(|f| parse_compacted_json_versions(f).map(|r| (r, f.clone())))
            .collect();
        parsed.sort_by_key(|b| std::cmp::Reverse(b.0 .1)); // sort by end_version descending
        let mut selected: Vec<((i64, i64), String)> = Vec::new();
        let mut covered_up_to: Option<i64> = None;
        for entry in parsed {
            if let Some(boundary) = covered_up_to {
                if entry.0 .1 >= boundary {
                    continue;
                }
            }
            covered_up_to = Some(entry.0 .0);
            selected.push(entry);
        }

        // Remove individual commits covered by selected compaction ranges.
        files.commit_files.retain(|f| {
            let Some(v) = parse_commit_version(f) else {
                return true;
            };
            !selected
                .iter()
                .any(|((start, end), _)| v >= *start && v <= *end)
        });
        let compaction_filenames: Vec<String> =
            selected.into_iter().map(|(_, name)| name).collect();
        files.commit_files.extend(compaction_filenames);
        files.commit_files.sort();
    }

    Ok(files)
}

fn merge_catalog_managed_commits_for_planner(
    files: &mut LogSegmentFiles,
    catalog_managed_commits: Option<&CatalogManagedCommitSet>,
) {
    let Some(catalog_managed_commits) = catalog_managed_commits else {
        return;
    };

    let latest_table_version = catalog_managed_commits.latest_table_version;
    if latest_table_version >= 0 {
        files.commit_files.retain(|file| {
            parse_log_file_version(file)
                .map(|version| version <= latest_table_version)
                .unwrap_or(true)
        });
        files.compaction_files.retain(|file| {
            parse_compacted_json_versions(file)
                .map(|(_, end)| end <= latest_table_version)
                .unwrap_or(true)
        });
    }

    let mut by_version = files
        .commit_files
        .drain(..)
        .filter_map(|file| parse_log_file_version(&file).map(|version| (version, file)))
        .collect::<BTreeMap<_, _>>();
    for commit in &catalog_managed_commits.commits {
        by_version.insert(
            commit.version,
            catalog_managed_commit_file_name(&commit.file_name),
        );
    }
    files.commit_files = by_version.into_values().collect();

    files.compaction_files.retain(|file| {
        let Some((start, end)) = parse_compacted_json_versions(file) else {
            return true;
        };
        !catalog_managed_commits
            .commits
            .iter()
            .any(|commit| commit.version >= start && commit.version <= end)
    });
}

fn parse_log_file_version(file: &str) -> Option<i64> {
    file.rsplit('/').next().and_then(parse_version_prefix)
}
