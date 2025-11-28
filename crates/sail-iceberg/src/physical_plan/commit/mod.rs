// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod commit_exec;

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
