// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/table/builder.rs>

use object_store::path::Path;
use serde::{Deserialize, Serialize};

/// Configuration options for loading Delta table snapshots.
///
/// Controls how the log-replay reads the `_delta_log`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogManagedCommitFile {
    pub version: i64,
    pub file_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogManagedCommitSet {
    pub latest_table_version: i64,
    pub commits: Vec<CatalogManagedCommitFile>,
}

impl CatalogManagedCommitSet {
    pub fn latest_replay_version(&self) -> Option<i64> {
        (self.latest_table_version >= 0).then_some(self.latest_table_version)
    }
}

pub(crate) fn catalog_managed_commit_path(file_name: &str) -> Path {
    let file_name = file_name.trim_start_matches('/');
    if let Some(index) = file_name.find("_delta_log/") {
        Path::from(&file_name[index..])
    } else if let Some(index) = file_name.find("_staged_commits/") {
        Path::from(format!("_delta_log/{}", &file_name[index..]))
    } else if file_name.starts_with("_delta_log/") {
        Path::from(file_name)
    } else if file_name.starts_with("_staged_commits/") {
        Path::from(format!("_delta_log/{file_name}"))
    } else {
        Path::from(format!("_delta_log/_staged_commits/{file_name}"))
    }
}

pub(crate) fn catalog_managed_commit_file_name(file_name: &str) -> String {
    catalog_managed_commit_path(file_name)
        .as_ref()
        .trim_start_matches("_delta_log/")
        .to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSnapshotConfig {
    /// Whether snapshots should eagerly load file level metadata.
    pub require_files: bool,
    /// Number of log files buffered concurrently when replaying the Delta log.
    pub log_buffer_size: usize,
    /// Number of log entries pulled per batch when materializing logs.
    pub log_batch_size: usize,
    /// Catalog-ratified commits for a catalog-managed Delta snapshot.
    pub catalog_managed_commits: Option<CatalogManagedCommitSet>,
}

impl Default for DeltaSnapshotConfig {
    fn default() -> Self {
        Self {
            require_files: true,
            log_buffer_size: default_parallelism() * 4,
            log_batch_size: 1024,
            catalog_managed_commits: None,
        }
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
