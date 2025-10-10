use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct StatisticsFile {
    /// The snapshot id of the statistics file.
    pub snapshot_id: i64,
    /// Path of the statistics file
    pub statistics_path: String,
    /// File size in bytes
    pub file_size_in_bytes: i64,
    /// File footer size in bytes
    pub file_footer_size_in_bytes: i64,
    /// Base64-encoded implementation-specific key metadata for encryption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<String>,
    /// Blob metadata
    pub blob_metadata: Vec<BlobMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct BlobMetadata {
    /// Type of the blob.
    pub r#type: String,
    /// Snapshot id of the blob.
    pub snapshot_id: i64,
    /// Sequence number of the blob.
    pub sequence_number: i64,
    /// Fields of the blob.
    pub fields: Vec<i32>,
    /// Properties of the blob.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub properties: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionStatisticsFile {
    /// The snapshot id of the statistics file.
    pub snapshot_id: i64,
    /// Path of the statistics file
    pub statistics_path: String,
    /// File size in bytes
    pub file_size_in_bytes: i64,
}
