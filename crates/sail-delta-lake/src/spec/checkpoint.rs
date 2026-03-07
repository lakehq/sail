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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::{Add, Metadata, Protocol, Remove, Transaction};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointActionRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<Add>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove: Option<Remove>,
    #[serde(
        rename = "metaData",
        alias = "metadata",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<Metadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Protocol>,
    #[serde(rename = "txn", skip_serializing_if = "Option::is_none")]
    pub txn: Option<Transaction>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpointHint {
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_of_add_files: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}
