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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/statistic_file.rs

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
