// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/snapshot.rs

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::spec::schema::SchemaId;

/// The ref name of the main branch of the table.
pub const MAIN_BRANCH: &str = "main";
pub const UNASSIGNED_SNAPSHOT_ID: i64 = -1;

/// Reference to [`Snapshot`].
pub type SnapshotRef = Arc<Snapshot>;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
/// The operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots.
pub enum Operation {
    /// Only data files were added and no files were removed.
    #[default]
    Append,
    /// Data and delete files were added and removed without changing table data;
    /// i.e., compaction, changing the data file format, or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    Delete,
}

impl Operation {
    /// Returns the string representation (lowercase) of the operation.
    pub fn as_str(&self) -> &str {
        match self {
            Operation::Append => "append",
            Operation::Replace => "replace",
            Operation::Overwrite => "overwrite",
            Operation::Delete => "delete",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table.
pub struct Snapshot {
    /// A unique long ID
    pub snapshot_id: i64,
    /// The snapshot ID of the snapshot's parent.
    /// Omitted for any snapshot with no parent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    pub sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    #[serde(default)]
    pub manifest_list: String,
    /// V1 snapshots list manifests directly instead of a manifest list file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifests: Option<Vec<String>>,
    /// A string map that summarizes the snapshot changes, including operation.
    pub summary: Summary,
    /// ID of the table's current schema when the snapshot was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<SchemaId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A reference’s snapshot and retention policy
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// Snapshot retention policy
    pub retention: SnapshotRetention,
}

impl SnapshotReference {
    /// Returns true if the snapshot reference is a branch.
    pub fn is_branch(&self) -> bool {
        matches!(self.retention, SnapshotRetention::Branch { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Snapshot retention policy
pub enum SnapshotRetention {
    /// Branches are mutable named references that can be updated by committing a new snapshot
    Branch {
        /// Minimum number of snapshots to keep in a branch while expiring snapshots.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        /// Max age of snapshots to keep when expiring, including the latest snapshot.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        /// Max age of the snapshot reference to keep while expiring snapshots.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
    /// Tags are labels for individual snapshots.
    Tag {
        /// Max age of the snapshot reference to keep while expiring snapshots.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Operation,
    /// Other summary data.
    #[serde(flatten)]
    pub additional_properties: HashMap<String, String>,
}

impl Summary {
    /// Create a new summary with the given operation.
    pub fn new(operation: Operation) -> Self {
        Self {
            operation,
            additional_properties: HashMap::new(),
        }
    }

    /// Add additional property to the summary.
    pub fn with_property(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.additional_properties
            .insert(key.to_string(), value.to_string());
        self
    }
}

impl Snapshot {
    /// Create a new snapshot builder.
    pub fn builder() -> SnapshotBuilder {
        SnapshotBuilder::new()
    }

    /// Get the id of the snapshot
    #[inline]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get parent snapshot id.
    #[inline]
    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.parent_snapshot_id
    }

    /// Get sequence_number of the snapshot. Is 0 for Iceberg V1 tables.
    #[inline]
    pub fn sequence_number(&self) -> i64 {
        self.sequence_number
    }

    /// Get location of manifest_list file
    #[inline]
    pub fn manifest_list(&self) -> &str {
        &self.manifest_list
    }

    /// Get V1 manifests list if present
    #[inline]
    pub fn manifests(&self) -> Option<&[String]> {
        self.manifests.as_deref()
    }

    /// Get summary of the snapshot
    #[inline]
    pub fn summary(&self) -> &Summary {
        &self.summary
    }

    /// Get the timestamp of when the snapshot was created
    #[inline]
    pub fn timestamp(&self) -> Result<DateTime<Utc>, String> {
        DateTime::from_timestamp_millis(self.timestamp_ms)
            .ok_or_else(|| format!("Invalid timestamp: {}", self.timestamp_ms))
    }

    /// Get the timestamp of when the snapshot was created in milliseconds
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Get the schema id of this snapshot.
    #[inline]
    pub fn schema_id(&self) -> Option<SchemaId> {
        self.schema_id
    }
}

/// Builder for creating snapshots.
#[derive(Debug)]
pub struct SnapshotBuilder {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    timestamp_ms: i64,
    manifest_list: Option<String>,
    summary: Option<Summary>,
    schema_id: Option<SchemaId>,
}

impl SnapshotBuilder {
    /// Create a new snapshot builder.
    pub fn new() -> Self {
        Self {
            snapshot_id: UNASSIGNED_SNAPSHOT_ID,
            parent_snapshot_id: None,
            sequence_number: 0,
            timestamp_ms: crate::utils::timestamp::monotonic_timestamp_ms(),
            manifest_list: None,
            summary: None,
            schema_id: None,
        }
    }

    /// Set the snapshot id.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }

    /// Set the parent snapshot id.
    pub fn with_parent_snapshot_id(mut self, parent_snapshot_id: i64) -> Self {
        self.parent_snapshot_id = Some(parent_snapshot_id);
        self
    }

    /// Set the sequence number.
    pub fn with_sequence_number(mut self, sequence_number: i64) -> Self {
        self.sequence_number = sequence_number;
        self
    }

    /// Set the timestamp in milliseconds.
    pub fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = timestamp_ms;
        self
    }

    /// Set the manifest list location.
    pub fn with_manifest_list(mut self, manifest_list: impl ToString) -> Self {
        self.manifest_list = Some(manifest_list.to_string());
        self
    }

    /// Set the summary.
    pub fn with_summary(mut self, summary: Summary) -> Self {
        self.summary = Some(summary);
        self
    }

    /// Set the schema id.
    pub fn with_schema_id(mut self, schema_id: SchemaId) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Build the snapshot.
    pub fn build(self) -> Result<Snapshot, String> {
        // For V1 compatibility allow manifest_list to be missing when manifests provided
        let manifest_list = self.manifest_list.unwrap_or_default();
        let summary = self
            .summary
            .unwrap_or_else(|| Summary::new(Operation::Append));

        Ok(Snapshot {
            snapshot_id: self.snapshot_id,
            parent_snapshot_id: self.parent_snapshot_id,
            sequence_number: self.sequence_number,
            timestamp_ms: self.timestamp_ms,
            manifest_list,
            manifests: None,
            summary,
            schema_id: self.schema_id,
        })
    }
}

impl Default for SnapshotBuilder {
    fn default() -> Self {
        Self::new()
    }
}
