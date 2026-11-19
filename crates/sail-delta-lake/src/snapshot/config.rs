// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/table/builder.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::spec::parse_commit_version;

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

/// Returns the path of a commit file returned by the catalog. The catalog may return a commit
/// after it is published, so a published file name (`<version>.json`) is a file of the Delta
/// log directory, and any other file name is a staged commit.
pub(crate) fn catalog_managed_commit_path(file_name: &str) -> Path {
    let file_name = file_name.trim_start_matches('/');
    if parse_commit_version(file_name).is_some() {
        Path::from(format!("_delta_log/{file_name}"))
    } else if let Some(index) = file_name.find("_delta_log/") {
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
