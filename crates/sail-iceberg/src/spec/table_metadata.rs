use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    FormatVersion, PartitionSpec, PartitionStatisticsFile, Schema, Snapshot, SnapshotReference,
    SortOrder, StatisticsFile,
};

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
        if let Some(snapshot_id) = self.current_snapshot_id {
            self.snapshots
                .iter()
                .find(|snapshot| snapshot.snapshot_id() == snapshot_id)
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
        log::debug!("[ICEBERG] Attempting to parse table metadata JSON");

        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(json_value) => {
                if let Some(obj) = json_value.as_object() {
                    log::debug!(
                        "[ICEBERG] JSON fields present: {:?}",
                        obj.keys().collect::<Vec<_>>()
                    );

                    if let Some(refs) = obj.get("refs") {
                        log::debug!("[ICEBERG] refs field: {:?}", refs);
                    }
                    if let Some(sort_orders) = obj.get("sort-orders") {
                        log::debug!("[ICEBERG] sort-orders field: {:?}", sort_orders);
                    }
                    if let Some(stats) = obj.get("statistics") {
                        log::debug!("[ICEBERG] statistics field: {:?}", stats);
                    }
                    if let Some(partition_stats) = obj.get("partition-statistics") {
                        log::debug!(
                            "[ICEBERG] partition-statistics field: {:?}",
                            partition_stats
                        );
                    }
                }

                log::debug!("[ICEBERG] Deserializing to TableMetadata struct");
                serde_json::from_value::<TableMetadataEnum>(json_value)
                    .map_err(|e| {
                        log::error!("[ICEBERG] Failed to deserialize TableMetadata: {:?}", e);
                        e
                    })
                    .map(|tm| match tm {
                        TableMetadataEnum::V1(t) | TableMetadataEnum::V2(t) => t,
                    })
            }
            Err(e) => {
                log::error!("[ICEBERG] Failed to parse as JSON: {:?}", e);
                Err(e)
            }
        }
    }

    /// Serialize table metadata to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}
