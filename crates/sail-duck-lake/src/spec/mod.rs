use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Schema identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaIndex(pub u64);

/// Table identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableIndex(pub u64);

/// Column/field identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FieldIndex(pub u64);

/// Data file identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataFileIndex(pub u64);

/// Delete file identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeleteFileIndex(pub u64);

/// Column mapping identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MappingIndex(pub u64);

/// Partition identifier in DuckLake metadata.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(pub u64);

/// Compaction operation type for optimizing DuckLake tables.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionType {
    MergeAdjacentTables,
    RewriteDeletes,
}

/// Cleanup operation type for removing obsolete files.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CleanupType {
    OldFiles,
    OrphanedFiles,
}

/// Encryption mode for DuckLake data files.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DuckLakeEncryption {
    Automatic,
    Encrypted,
    Unencrypted,
}

/// Snapshot metadata from `ducklake_snapshot` and `ducklake_snapshot_changes` tables.
/// Represents a point-in-time state of the DuckLake catalog.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SnapshotInfo {
    pub snapshot_id: u64,
    pub snapshot_time: DateTime<Utc>,
    pub schema_version: u64,
    pub next_catalog_id: u64,
    pub next_file_id: u64,
    pub changes_made: Option<String>,
    pub author: Option<String>,
    pub commit_message: Option<String>,
    pub commit_extra_info: Option<String>,
}

/// Schema metadata from `ducklake_schema` table.
/// Represents a namespace containing tables in DuckLake.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    pub schema_id: SchemaIndex,
    pub schema_uuid: Uuid,
    pub begin_snapshot: Option<u64>,
    pub end_snapshot: Option<u64>,
    pub schema_name: String,
    pub path: String,
    pub path_is_relative: bool,
}

/// Column metadata from `ducklake_column` table.
/// Supports nested columns via `parent_column` field for complex types.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub column_id: FieldIndex,
    pub begin_snapshot: Option<u64>,
    pub end_snapshot: Option<u64>,
    pub table_id: TableIndex,
    pub column_order: u64,
    pub column_name: String,
    pub column_type: String,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub nulls_allowed: bool,
    pub parent_column: Option<FieldIndex>,
}

/// Table metadata from `ducklake_table` table.
/// Aggregates columns and inlined data tables for the complete table definition.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub table_id: TableIndex,
    pub table_uuid: Uuid,
    pub begin_snapshot: Option<u64>,
    pub end_snapshot: Option<u64>,
    pub schema_id: SchemaIndex,
    pub table_name: String,
    pub path: String,
    pub path_is_relative: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inlined_data_tables: Vec<InlinedTableInfo>,
}

/// Inlined data table metadata from `ducklake_inlined_data_tables` table.
/// References small tables stored inline within the metadata database.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InlinedTableInfo {
    pub table_name: String,
    pub schema_version: u64,
}

/// Data file metadata from `ducklake_data_file` table.
/// Describes a Parquet or other format file containing table data.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub data_file_id: DataFileIndex,
    pub table_id: TableIndex,
    pub begin_snapshot: Option<u64>,
    pub end_snapshot: Option<u64>,
    pub file_order: u64,
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: Option<String>,
    pub record_count: u64,
    pub file_size_bytes: u64,
    pub footer_size: Option<u64>,
    pub row_id_start: Option<u64>,
    pub partition_id: Option<PartitionId>,
    pub encryption_key: String,
    pub partial_file_info: Option<String>,
    pub mapping_id: MappingIndex,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_stats: Vec<ColumnStatsInfo>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_values: Vec<FilePartitionInfo>,
}

/// Column statistics from `ducklake_file_column_stats` table.
/// Provides per-column statistics for query optimization and pruning.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ColumnStatsInfo {
    pub column_id: FieldIndex,
    pub column_size_bytes: Option<u64>,
    pub value_count: Option<u64>,
    pub null_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub contains_nan: Option<bool>,
    pub extra_stats: Option<String>,
}

/// Partition value from `ducklake_file_partition_value` table.
/// Associates a data file with specific partition key values.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilePartitionInfo {
    pub partition_key_index: u64,
    pub partition_value: String,
}

/// Delete file metadata from `ducklake_delete_file` table.
/// Tracks files containing row-level deletes for a data file.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DeleteFileInfo {
    pub delete_file_id: DeleteFileIndex,
    pub table_id: TableIndex,
    pub begin_snapshot: Option<u64>,
    pub end_snapshot: Option<u64>,
    pub data_file_id: DataFileIndex,
    pub path: String,
    pub path_is_relative: bool,
    pub format: Option<String>,
    pub delete_count: u64,
    pub file_size_bytes: u64,
    pub footer_size: u64,
    pub encryption_key: String,
}

/// Column mapping metadata from `ducklake_column_mapping` table.
/// Defines how source file columns map to table schema for schema evolution.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct NameMapping {
    pub mapping_id: MappingIndex,
    pub table_id: TableIndex,
    pub map_type: String,
}

/// Individual name mapping entry from `ducklake_name_mapping` table.
/// Maps a source column name to a target field ID in the table schema.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct NameMappingEntry {
    pub mapping_id: MappingIndex,
    pub column_id: FieldIndex,
    pub source_name: String,
    pub target_field_id: FieldIndex,
    pub parent_column: Option<FieldIndex>,
    pub is_partition: bool,
}

/// Key-value tag for metadata annotation.
/// Used in `ducklake_tag` and `ducklake_column_tag` tables.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DuckLakeTag {
    pub key: String,
    pub value: String,
}

/// Configuration option from `ducklake_metadata` table.
/// Stores catalog-level, schema-level, or table-level configuration.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DuckLakeConfigOption {
    pub key: String,
    pub value: String,
    pub scope: Option<String>,
    pub scope_id: Option<u64>,
}
