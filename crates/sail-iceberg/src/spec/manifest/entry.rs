use serde::{Deserialize, Serialize};

use super::DataFile;

/// Status of a manifest entry.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ManifestStatus {
    Added,
    Existing,
    Deleted,
}

/// A manifest entry represents a data file in a manifest.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub status: ManifestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

impl ManifestEntry {
    pub fn new(
        status: ManifestStatus,
        snapshot_id: Option<i64>,
        sequence_number: Option<i64>,
        file_sequence_number: Option<i64>,
        data_file: DataFile,
    ) -> Self {
        Self {
            status,
            snapshot_id,
            sequence_number,
            file_sequence_number,
            data_file,
        }
    }
}
