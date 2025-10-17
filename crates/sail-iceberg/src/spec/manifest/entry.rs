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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/entry.rs

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
