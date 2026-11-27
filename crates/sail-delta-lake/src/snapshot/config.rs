// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/table/builder.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use object_store::path::Path;
use sail_common_datafusion::session::time_zone::session_time_zone;
use sail_common_datafusion::utils::datetime::arrow_timezone_label;
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
    /// Replay transaction actions even when the checksum omits them.
    pub require_transactions: bool,
    /// Number of log files buffered concurrently when replaying the Delta log.
    pub log_buffer_size: usize,
    /// Number of log entries pulled per batch when materializing logs.
    pub log_batch_size: usize,
    /// Catalog-ratified commits for a catalog-managed Delta snapshot.
    pub catalog_managed_commits: Option<CatalogManagedCommitSet>,
    /// The time zone of TIMESTAMP partition values without an offset (see
    /// [`partition_time_zone`]).
    pub partition_time_zone: Arc<str>,
}

impl DeltaSnapshotConfig {
    pub fn with_partition_time_zone(self, options: &ConfigOptions) -> Self {
        Self {
            partition_time_zone: partition_time_zone(options),
            ..self
        }
    }
}

/// Returns the time zone in which a TIMESTAMP partition value without an offset is read: the
/// session time zone, as Delta-Spark reads it.
pub fn partition_time_zone(options: &ConfigOptions) -> Arc<str> {
    session_time_zone(options)
        .map(|time_zone| arrow_timezone_label(time_zone).into())
        .unwrap_or_else(|| "UTC".into())
}

impl Default for DeltaSnapshotConfig {
    fn default() -> Self {
        Self {
            require_files: true,
            require_transactions: false,
            log_buffer_size: default_parallelism() * 4,
            log_batch_size: 1024,
            catalog_managed_commits: None,
            partition_time_zone: "UTC".into(),
        }
    }
}

fn default_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
