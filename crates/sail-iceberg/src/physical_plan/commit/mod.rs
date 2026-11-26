// SPDX-License-Identifier: Apache-2.0

pub mod commit_exec;

use std::collections::BTreeMap;

use sail_common_datafusion::catalog::LakehouseExecutionContext;
use serde::{Deserialize, Serialize};

use crate::operations::SnapshotUpdateKind;
use crate::spec::{DataFile, PartitionSpec, Schema, TableRequirement, TableUpdate};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcebergCommitInfo {
    pub table_uri: String,
    pub row_count: u64,
    pub action_summary: Option<crate::operation_metrics::IcebergActionSummary>,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub manifest_path: String,
    pub manifest_list_path: String,
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
    pub table_properties: Vec<(String, String)>,
    #[serde(default)]
    pub snapshot_properties: BTreeMap<String, String>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
    #[serde(default)]
    pub dynamic_partition_overwrite: bool,
    pub snapshot_update_kind: SnapshotUpdateKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_spec: Option<PartitionSpec>,
}
