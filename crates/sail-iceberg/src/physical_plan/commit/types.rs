use serde::{Deserialize, Serialize};

use crate::spec::{DataFile, Operation, PartitionSpec, Schema, TableRequirement, TableUpdate};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcebergCommitInfo {
    pub table_uri: String,
    pub row_count: u64,
    pub data_files: Vec<DataFile>,
    pub manifest_path: String,
    pub manifest_list_path: String,
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
    pub operation: Operation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<PartitionSpec>,
}
