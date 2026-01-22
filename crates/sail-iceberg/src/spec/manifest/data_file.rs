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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/data_file.rs

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::spec::types::values::Literal;
use crate::spec::Datum;

/// Content type of a data file.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

impl DataContentType {
    pub const fn as_action_str(self) -> &'static str {
        match self {
            Self::Data => "Data",
            Self::PositionDeletes => "PositionDeletes",
            Self::EqualityDeletes => "EqualityDeletes",
        }
    }
}

/// File format of a data file.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFileFormat {
    Avro,
    Orc,
    Parquet,
    Puffin,
}

impl DataFileFormat {
    pub const fn as_action_str(self) -> &'static str {
        match self {
            Self::Avro => "Avro",
            Self::Orc => "Orc",
            Self::Parquet => "Parquet",
            Self::Puffin => "Puffin",
        }
    }
}

/// A data file in Iceberg.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub content: DataContentType,
    pub file_path: String,
    pub file_format: DataFileFormat,
    pub partition: Vec<Option<Literal>>,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub column_sizes: HashMap<i32, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub value_counts: HashMap<i32, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub null_value_counts: HashMap<i32, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub nan_value_counts: HashMap<i32, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub lower_bounds: HashMap<i32, Datum>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub upper_bounds: HashMap<i32, Datum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub split_offsets: Vec<i64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub equality_ids: Vec<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    pub partition_spec_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_in_bytes: Option<i64>,
}

impl DataFile {
    pub fn content_type(&self) -> DataContentType {
        self.content
    }
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
    pub fn file_format(&self) -> DataFileFormat {
        self.file_format
    }
    pub fn partition(&self) -> &[Option<Literal>] {
        &self.partition
    }
    pub fn record_count(&self) -> u64 {
        self.record_count
    }
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    pub fn column_sizes(&self) -> &HashMap<i32, u64> {
        &self.column_sizes
    }
    pub fn value_counts(&self) -> &HashMap<i32, u64> {
        &self.value_counts
    }
    pub fn null_value_counts(&self) -> &HashMap<i32, u64> {
        &self.null_value_counts
    }
    pub fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        &self.nan_value_counts
    }
    pub fn lower_bounds(&self) -> &HashMap<i32, Datum> {
        &self.lower_bounds
    }
    pub fn upper_bounds(&self) -> &HashMap<i32, Datum> {
        &self.upper_bounds
    }
}
