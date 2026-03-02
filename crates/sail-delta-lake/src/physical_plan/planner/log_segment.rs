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
use futures::TryStreamExt;
use object_store::path::Path;
use serde::Deserialize;

use super::context::PlannerContext;

const DELTA_LOG_DIR: &str = "_delta_log";
const LAST_CHECKPOINT_FILE: &str = "_last_checkpoint";

#[derive(Debug, Clone, Default)]
pub struct LogSegmentFiles {
    pub checkpoint_files: Vec<String>,
    pub commit_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LastCheckpointHint {
    version: i64,
}

fn parse_version_prefix(filename: &str) -> Option<i64> {
    let prefix = filename.get(0..20)?;
    if !prefix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    prefix.parse::<i64>().ok()
}

fn parse_commit_version(filename: &str) -> Option<i64> {
    if filename.len() != 25 || !filename.ends_with(".json") {
        return None;
    }
    parse_version_prefix(filename)
}

fn parse_checkpoint_version(filename: &str) -> Option<i64> {
    if !filename.contains(".checkpoint") || !filename.ends_with(".parquet") {
        return None;
    }
    parse_version_prefix(filename)
}

fn join_path(base: &str, suffix: &str) -> String {
    if base.ends_with('/') {
        format!("{base}{suffix}")
    } else {
        format!("{base}/{suffix}")
    }
}

async fn read_last_checkpoint_version(ctx: &PlannerContext<'_>) -> Option<i64> {
    let log_store = ctx.log_store().ok()?;
    let store = log_store.object_store(None);
    let table_root_path = log_store.config().location.path();
    let log_root_path = join_path(table_root_path, DELTA_LOG_DIR);
    let path = Path::from(join_path(&log_root_path, LAST_CHECKPOINT_FILE));
    let bytes = store.get(&path).await.ok()?.bytes().await.ok()?;
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes).ok()?;
    Some(hint.version)
}

pub async fn list_log_segment_files(
    ctx: &PlannerContext<'_>,
    max_version: i64,
) -> Result<LogSegmentFiles> {
    let log_store = ctx.log_store()?;
    let store = log_store.object_store(None);
    let table_root_path = log_store.config().location.path();
    let log_root_path = join_path(table_root_path, DELTA_LOG_DIR);
    let log_root = Path::from(log_root_path.clone());
    let offset_version = read_last_checkpoint_version(ctx)
        .await
        .map(|v| v.min(max_version).saturating_sub(1))
        .unwrap_or(0);
    let offset = Path::from(format!("{log_root_path}/{offset_version:020}"));

    // Prefer offset listing from `_last_checkpoint`, then fall back to full listing if unsupported.
    let mut entries = match store
        .list_with_offset(Some(&log_root), &offset)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(entries) => entries,
        Err(_) => store.list(Some(&log_root)).try_collect::<Vec<_>>().await?,
    };
    if entries.is_empty() && offset_version > 0 {
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
