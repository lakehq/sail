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

use futures::TryStreamExt;

use crate::spec::{
    delta_log_prefix_path, delta_log_root_path, last_checkpoint_path, parse_checkpoint_version,
    parse_commit_version, parse_version_prefix, DeltaResult, LastCheckpointHint,
};
use crate::storage::LogStoreRef;

/// The minimal set of Delta log files needed to reconstruct table state up to a given version.
#[derive(Debug, Clone, Default)]
pub struct LogSegmentFiles {
    /// Parquet checkpoint files for the latest checkpoint at or before `max_version`.
    pub checkpoint_files: Vec<String>,
    /// Commit JSON files sorted by version, strictly newer than the latest checkpoint.
    pub commit_files: Vec<String>,
}

/// Options controlling which commit files are included in the resolved segment.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogSegmentResolveOptions {
    /// If set, only include commit JSON files whose version falls within `[start, end]`.
    pub commit_version_range: Option<(i64, i64)>,
}

async fn read_last_checkpoint_version(log_store: &dyn crate::storage::LogStore) -> Option<i64> {
    let store = log_store.object_store(None);
    let path = last_checkpoint_path();
    let bytes = store.get(&path).await.ok()?.bytes().await.ok()?;
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes).ok()?;
    Some(hint.version)
}

/// List all Delta log files up to `max_version` from the given log store.
///
/// Returns a [`LogSegmentFiles`] containing:
/// - all parquet files belonging to the **latest** checkpoint at or before `max_version`
/// - all commit JSON files at or before `max_version`
///
/// Commit files are **not** filtered against the checkpoint here; call
/// [`resolve_log_segment_files`] if you need the minimal replay set.
pub async fn list_log_segment_files(
    log_store: &LogStoreRef,
    max_version: i64,
) -> DeltaResult<LogSegmentFiles> {
    let store = log_store.object_store(None);
    let log_root = delta_log_root_path();
    let offset_version = read_last_checkpoint_version(log_store.as_ref())
        .await
        .map(|v| v.min(max_version).saturating_sub(1))
        .unwrap_or(0);
    let offset = delta_log_prefix_path(offset_version);

    // Prefer offset listing from `_last_checkpoint`, then fall back to full listing if unsupported.
    let mut entries = match store
        .list_with_offset(Some(&log_root), &offset)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(entries) => entries,
        Err(_) => store.list(Some(&log_root)).try_collect::<Vec<_>>().await?,
    };
    // Some object stores treat `list_with_offset` as strictly greater-than and can
    // skip the exact `offset` object. For small tables (only version 0 commit),
    // that may return empty even though files exist. Fall back to full listing.
    if entries.is_empty() {
        entries = store.list(Some(&log_root)).try_collect::<Vec<_>>().await?;
    }

    let mut checkpoint_candidates: Vec<(i64, String)> = Vec::new();
    let mut commit_candidates: Vec<(i64, String)> = Vec::new();

    for meta in entries {
        let filename = match meta.location.as_ref().rsplit('/').next() {
            Some(name) => name,
            None => continue,
        };
        if let Some(version) = parse_checkpoint_version(filename) {
            if version <= max_version {
                checkpoint_candidates.push((version, filename.to_string()));
            }
            continue;
        }
        if let Some(version) = parse_commit_version(filename) {
            if version <= max_version {
                commit_candidates.push((version, filename.to_string()));
            }
        }
    }

    let latest_checkpoint_version = checkpoint_candidates
        .iter()
        .map(|(version, _)| *version)
        .max();
    let mut checkpoint_files = match latest_checkpoint_version {
        Some(version) => checkpoint_candidates
            .into_iter()
            .filter_map(|(file_version, filename)| (file_version == version).then_some(filename))
            .collect::<Vec<_>>(),
        None => Vec::new(),
    };
    checkpoint_files.sort();

    commit_candidates.sort_by(|(av, af), (bv, bf)| av.cmp(bv).then_with(|| af.cmp(bf)));
    let commit_files = commit_candidates
        .into_iter()
        .map(|(_, filename)| filename)
        .collect::<Vec<_>>();

    Ok(LogSegmentFiles {
        checkpoint_files,
        commit_files,
    })
}

/// Resolve the minimal set of Delta log files needed to replay table state up to `max_version`.
///
/// Unlike [`list_log_segment_files`], this function:
/// 1. Filters out commit JSON files that are already covered by the latest checkpoint.
/// 2. Optionally restricts commit files to a specific version range via `options`.
pub async fn resolve_log_segment_files(
    log_store: &LogStoreRef,
    max_version: i64,
    options: LogSegmentResolveOptions,
) -> DeltaResult<LogSegmentFiles> {
    let mut files = list_log_segment_files(log_store, max_version).await?;

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
