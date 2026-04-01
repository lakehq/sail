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

pub(crate) use crate::delta_log::ReplayedTableHeader;
use crate::delta_log::{list_log_files, read_last_checkpoint_version_from_store};
use crate::kernel::checkpoints::read_checkpoint_main_rows_from_parquet;
use crate::spec::{is_uuid_checkpoint_filename, DeltaResult};

/// The minimal set of Delta log files needed to reconstruct table state up to a given version.
#[derive(Debug, Clone, Default)]
pub struct LogSegmentFiles {
    /// Parquet checkpoint files for the latest checkpoint at or before `max_version`.
    pub checkpoint_files: Vec<String>,
    /// Commit JSON files sorted by version, strictly newer than the latest checkpoint.
    pub commit_files: Vec<String>,
    /// V2 checkpoint sidecar files referenced by the latest checkpoint.
    /// Paths are relative to `_delta_log/` (e.g. `_sidecars/uuid.parquet`).
    pub sidecar_files: Vec<String>,
    /// Compacted JSON files that aggregate multiple commit versions.
    /// Filenames follow the pattern `{start:020}.{end:020}.compacted.json`.
    pub compaction_files: Vec<String>,
}

/// Options controlling which commit files are included in the resolved segment.
#[derive(Debug, Clone, Copy, Default)]
pub struct LogSegmentResolveOptions {
    /// If set, only include commit JSON files whose version falls within `[start, end]`.
    pub commit_version_range: Option<(i64, i64)>,
}

/// List all Delta log files up to `max_version` from the given log store.
///
/// Returns a [`LogSegmentFiles`] containing:
/// - all parquet files belonging to the **latest** checkpoint at or before `max_version`
/// - all commit JSON files at or before `max_version`
///
/// Commit files are **not** filtered against the checkpoint here.
pub async fn list_log_segment_files(
    log_store: &crate::storage::LogStoreRef,
    max_version: i64,
) -> DeltaResult<LogSegmentFiles> {
    let store = log_store.object_store(None);
    let offset_version = read_last_checkpoint_version_from_store(store.clone())
        .await
        .map(|v| v.min(max_version).saturating_sub(1))
        .unwrap_or(0);

    let (_, checkpoint_meta, commit_metas, compaction_metas) =
        list_log_files(store.clone(), offset_version, max_version).await?;

    let mut checkpoint_files: Vec<String> = Vec::new();
    let mut sidecar_files: Vec<String> = Vec::new();

    if let Some(meta) = checkpoint_meta {
        let filename = meta
            .location
            .as_ref()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        checkpoint_files.push(filename.clone());

        // Only UUID-named checkpoints (V2) can contain sidecar references.
        if is_uuid_checkpoint_filename(&filename) {
            let rows = read_checkpoint_main_rows_from_parquet(store, meta).await?;
            for row in &rows {
                if let Some(ref sidecar) = row.sidecar {
                    sidecar_files.push(format!("_sidecars/{}", sidecar.path));
                }
            }
        }
    }

    checkpoint_files.sort();
    sidecar_files.sort();

    let mut commit_files: Vec<String> = commit_metas
        .into_iter()
        .filter_map(|(_, meta)| {
            meta.location
                .as_ref()
                .rsplit('/')
                .next()
                .map(|s| s.to_string())
        })
        .collect();
    commit_files.sort();

    let mut compaction_files: Vec<String> = compaction_metas
        .into_iter()
        .filter_map(|(_, meta)| {
            meta.location
                .as_ref()
                .rsplit('/')
                .next()
                .map(|s| s.to_string())
        })
        .collect();
    compaction_files.sort();

    Ok(LogSegmentFiles {
        checkpoint_files,
        commit_files,
        sidecar_files,
        compaction_files,
    })
}
