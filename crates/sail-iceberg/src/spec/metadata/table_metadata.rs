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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/table_metadata.rs

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::spec::encrypted_key::EncryptedKey;
use crate::spec::metadata::format::FormatVersion;
use crate::spec::metadata::statistic_file::{PartitionStatisticsFile, StatisticsFile};
use crate::spec::partition::PartitionSpec;
use crate::spec::schema::Schema;
use crate::spec::snapshots::{Snapshot, SnapshotReference, MAIN_BRANCH};
use crate::spec::sort::SortOrder;

/// Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Integer Version for the format
    pub format_version: FormatVersion,
    /// A UUID that identifies the table
    pub table_uuid: Option<Uuid>,
    /// Location tables base location
    pub location: String,
    /// The tables highest sequence number
    #[serde(default)]
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id
    pub schemas: Vec<Schema>,
    /// ID of the table's current schema
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects
    #[serde(default)]
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the "current" spec that writers should use by default
    #[serde(default)]
    pub default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table
    #[serde(default)]
    pub last_partition_id: i32,
    /// A string to string map of table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// long ID of the current table snapshot
    pub current_snapshot_id: Option<i64>,
    /// A long higher than all assigned row IDs; the next snapshot's first-row-id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,
    /// Iceberg v3 encrypted table keys. We preserves this metadata but does not decrypt data yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encryption_keys: Vec<EncryptedKey>,
    /// A list of valid snapshots
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    /// A list of timestamp and snapshot ID pairs that encodes changes to the current snapshot for the table
    #[serde(default)]
    pub snapshot_log: Vec<SnapshotLog>,
    /// A list of timestamp and metadata file location pairs that encodes changes to the previous metadata files for the table
    #[serde(default)]
    pub metadata_log: Vec<MetadataLog>,
    /// Sort orders for the table
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order ID
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_sort_order_id: Option<i32>,
    /// Named references to snapshots
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub refs: HashMap<String, SnapshotReference>,
    /// Statistics files
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statistics: Vec<StatisticsFile>,
    /// Partition statistics files
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_statistics: Vec<PartitionStatisticsFile>,
}

/// Snapshot log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    /// Timestamp when the snapshot became current
    pub timestamp_ms: i64,
    /// Snapshot ID
    pub snapshot_id: i64,
}

/// Metadata log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    /// Timestamp when the metadata file was created
    pub timestamp_ms: i64,
    /// Location of the metadata file
    pub metadata_file: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TableMetadataEnum {
    V1(TableMetadata),
    V2(TableMetadata),
    V3(TableMetadata),
}

impl TableMetadata {
    /// Get the current schema
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schemas
            .iter()
            .find(|schema| schema.schema_id() == self.current_schema_id)
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let snapshot_id = self
            .refs
            .get(MAIN_BRANCH)
            .map(|r| {
                log::trace!("Using snapshot ID from refs[main]: {}", r.snapshot_id);
                r.snapshot_id
            })
            .or_else(|| {
                if let Some(id) = self.current_snapshot_id {
                    log::trace!("Fallback to current_snapshot_id: {}", id);
                }
                self.current_snapshot_id
            });

        if let Some(sid) = snapshot_id {
            self.snapshots
                .iter()
                .find(|snapshot| snapshot.snapshot_id() == sid)
        } else {
            None
        }
    }

    /// Get the default partition spec
    pub fn default_partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_specs
            .iter()
            .find(|spec| spec.spec_id() == self.default_spec_id)
    }

    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        log::trace!("Attempting to parse table metadata JSON");

        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(json_value) => {
                if let Some(obj) = json_value.as_object() {
                    log::trace!("JSON fields present: {:?}", obj.keys().collect::<Vec<_>>());

                    if let Some(refs) = obj.get("refs") {
                        log::trace!("refs field: {:?}", refs);
                    }
                    if let Some(sort_orders) = obj.get("sort-orders") {
                        log::trace!("sort-orders field: {:?}", sort_orders);
                    }
                    if let Some(stats) = obj.get("statistics") {
                        log::trace!("statistics field: {:?}", stats);
                    }
                    if let Some(partition_stats) = obj.get("partition-statistics") {
                        log::trace!("partition-statistics field: {:?}", partition_stats);
                    }
                }

                log::trace!("Deserializing to TableMetadata struct");
                serde_json::from_value::<TableMetadataEnum>(json_value)
                    .map_err(|e| {
                        log::trace!("Failed to deserialize TableMetadata: {:?}", e);
                        e
                    })
                    .map(|tm| match tm {
                        TableMetadataEnum::V1(t)
                        | TableMetadataEnum::V2(t)
                        | TableMetadataEnum::V3(t) => t,
                    })
            }
            Err(e) => {
                log::trace!("Failed to parse as JSON: {:?}", e);
                Err(e)
            }
        }
    }

    pub fn ensure_required_format_fields(&mut self) {
        if self.format_version >= FormatVersion::V2 {
            if self.table_uuid.is_none() {
                self.table_uuid = Some(Uuid::new_v4());
            }
            if self.sort_orders.is_empty() {
                self.sort_orders.push(SortOrder::unsorted_order());
            }
            if self.default_sort_order_id.is_none() {
                self.default_sort_order_id = Some(SortOrder::unsorted_order().order_id as i32);
            }
        }

        if self.format_version >= FormatVersion::V3 && self.next_row_id.is_none() {
            self.next_row_id = Some(self.inferred_next_row_id());
        }
    }

    pub fn row_lineage_start_row_id(&mut self) -> Option<i64> {
        self.ensure_required_format_fields();
        (self.format_version >= FormatVersion::V3).then(|| self.next_row_id.unwrap_or(0))
    }

    pub fn advance_next_row_id(&mut self, added_rows: i64) {
        if let Some(start_row_id) = self.row_lineage_start_row_id() {
            self.next_row_id = Some(start_row_id + added_rows);
        }
    }

    fn inferred_next_row_id(&self) -> i64 {
        self.snapshots
            .iter()
            .filter_map(
                |snapshot| match (snapshot.first_row_id, snapshot.added_rows) {
                    (Some(first_row_id), Some(added_rows)) => Some(first_row_id + added_rows),
                    _ => None,
                },
            )
            .max()
            .unwrap_or(0)
    }

    /// Serialize table metadata to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut metadata = self.clone();
        metadata.ensure_required_format_fields();
        serde_json::to_vec(&metadata)
    }
}
