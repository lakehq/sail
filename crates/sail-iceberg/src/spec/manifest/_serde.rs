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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/_serde.rs

use apache_avro::types::Value as AvroValue;
use serde::{Deserialize, Serialize};

use super::{DataContentType, DataFile, DataFileFormat};
use crate::spec::Schema;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ManifestEntryAvro {
    #[serde(rename = "status")]
    pub status: i32,
    #[serde(rename = "snapshot_id")]
    pub snapshot_id: Option<i64>,
    #[serde(rename = "sequence_number")]
    pub sequence_number: Option<i64>,
    #[serde(rename = "file_sequence_number")]
    pub file_sequence_number: Option<i64>,
    #[serde(rename = "data_file")]
    pub data_file: DataFileAvro,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DataFileAvro {
    #[serde(rename = "content", default)]
    pub content: i32,
    #[serde(rename = "file_path")]
    pub file_path: String,
    #[serde(rename = "file_format")]
    pub file_format: String,
    #[serde(rename = "partition")]
    pub partition: serde_json::Value,
    #[serde(rename = "record_count")]
    pub record_count: i64,
    #[serde(rename = "file_size_in_bytes")]
    pub file_size_in_bytes: i64,
    #[serde(skip)]
    pub column_sizes: Option<AvroValue>,
    #[serde(skip)]
    pub value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub null_value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub nan_value_counts: Option<AvroValue>,
    #[serde(skip)]
    pub lower_bounds: Option<AvroValue>,
    #[serde(skip)]
    pub upper_bounds: Option<AvroValue>,
    #[serde(rename = "key_metadata")]
    pub key_metadata: Option<Vec<u8>>,
    #[serde(rename = "split_offsets")]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(rename = "equality_ids")]
    pub equality_ids: Option<Vec<i32>>,
    #[serde(rename = "sort_order_id")]
    pub sort_order_id: Option<i32>,
}

impl DataFileAvro {
    pub fn into_data_file(
        self,
        schema: &Schema,
        _partition_type_len: i32,
        partition_spec_id: i32,
    ) -> DataFile {
        let content = match self.content {
            0 => DataContentType::Data,
            1 => DataContentType::PositionDeletes,
            2 => DataContentType::EqualityDeletes,
            _ => DataContentType::Data,
        };

        let file_format = match self.file_format.to_uppercase().as_str() {
            "PARQUET" => DataFileFormat::Parquet,
            "AVRO" => DataFileFormat::Avro,
            "ORC" => DataFileFormat::Orc,
            _ => DataFileFormat::Parquet,
        };

        let partition = super::super::manifest::parse_partition_values(Some(&self.partition));

        let column_sizes = super::super::manifest::parse_i64_map_from_avro(&self.column_sizes)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let value_counts = super::super::manifest::parse_i64_map_from_avro(&self.value_counts)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let null_value_counts =
            super::super::manifest::parse_i64_map_from_avro(&self.null_value_counts)
                .into_iter()
                .map(|(k, v)| (k, v as u64))
                .collect();
        let nan_value_counts =
            super::super::manifest::parse_i64_map_from_avro(&self.nan_value_counts)
                .into_iter()
                .map(|(k, v)| (k, v as u64))
                .collect();

        let lower_bounds_raw =
            super::super::manifest::parse_bytes_map_from_avro(&self.lower_bounds);
        let upper_bounds_raw =
            super::super::manifest::parse_bytes_map_from_avro(&self.upper_bounds);
        let lower_bounds =
            super::super::manifest::parse_bounds_from_binary(lower_bounds_raw.as_ref(), schema);
        let upper_bounds =
            super::super::manifest::parse_bounds_from_binary(upper_bounds_raw.as_ref(), schema);

        DataFile {
            content,
            file_path: self.file_path,
            file_format,
            partition,
            record_count: self.record_count as u64,
            file_size_in_bytes: self.file_size_in_bytes as u64,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
            block_size_in_bytes: None,
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets.unwrap_or_default(),
            equality_ids: self.equality_ids.unwrap_or_default().into_iter().collect(),
            sort_order_id: self.sort_order_id,
            first_row_id: None,
            partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
}

// Note: We currently omit metrics maps serialization on write (left as None),
// and default them to empty on read. Partition is left as null to match the
// minimal writer schema used in this crate; we will upgrade to full struct
// encoding in a subsequent step.

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV2 {
    pub status: i32,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFileSerde,
}

#[derive(Serialize, Deserialize)]
pub(super) struct ManifestEntryV1 {
    pub status: i32,
    pub snapshot_id: i64,
    pub data_file: DataFileSerde,
}

#[derive(Serialize, Deserialize)]
pub(super) struct DataFileSerde {
    #[serde(default)]
    pub content: i32,
    pub file_path: String,
    pub file_format: String,
    // Placeholder for partition struct; currently serialized as null
    #[serde(skip)]
    pub partition: Option<AvroValue>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_in_bytes: Option<i64>,
}

impl ManifestEntryV2 {
    pub fn from_entry(entry: super::ManifestEntry) -> Self {
        Self {
            status: match entry.status {
                super::ManifestStatus::Added => 1,
                super::ManifestStatus::Deleted => 2,
                super::ManifestStatus::Existing => 0,
            },
            snapshot_id: entry.snapshot_id,
            sequence_number: entry.sequence_number,
            file_sequence_number: entry.file_sequence_number,
            data_file: DataFileSerde::from_data_file(entry.data_file),
        }
    }

    pub fn into_entry(self, partition_spec_id: i32) -> super::ManifestEntry {
        let status = match self.status {
            1 => super::ManifestStatus::Added,
            2 => super::ManifestStatus::Deleted,
            _ => super::ManifestStatus::Existing,
        };
        super::ManifestEntry::new(
            status,
            self.snapshot_id,
            self.sequence_number,
            self.file_sequence_number,
            self.data_file.into_data_file(partition_spec_id),
        )
    }
}

impl ManifestEntryV1 {
    pub fn from_entry(entry: super::ManifestEntry) -> Self {
        Self {
            status: match entry.status {
                super::ManifestStatus::Added => 1,
                super::ManifestStatus::Deleted => 2,
                super::ManifestStatus::Existing => 0,
            },
            snapshot_id: entry.snapshot_id.unwrap_or_default(),
            data_file: DataFileSerde::from_data_file(entry.data_file),
        }
    }
}

impl DataFileSerde {
    pub fn from_data_file(df: super::DataFile) -> Self {
        Self {
            content: match df.content {
                DataContentType::Data => 0,
                DataContentType::PositionDeletes => 1,
                DataContentType::EqualityDeletes => 2,
            },
            file_path: df.file_path,
            file_format: match df.file_format {
                DataFileFormat::Parquet => "PARQUET".to_string(),
                DataFileFormat::Avro => "AVRO".to_string(),
                DataFileFormat::Orc => "ORC".to_string(),
                DataFileFormat::Puffin => "PUFFIN".to_string(),
            },
            partition: None, // see module note above
            record_count: df.record_count as i64,
            file_size_in_bytes: df.file_size_in_bytes as i64,
            key_metadata: df.key_metadata,
            split_offsets: if df.split_offsets.is_empty() {
                None
            } else {
                Some(df.split_offsets)
            },
            equality_ids: if df.equality_ids.is_empty() {
                None
            } else {
                Some(df.equality_ids)
            },
            sort_order_id: df.sort_order_id,
            first_row_id: df.first_row_id,
            referenced_data_file: df.referenced_data_file,
            content_offset: df.content_offset,
            content_size_in_bytes: df.content_size_in_bytes,
        }
    }

    pub fn into_data_file(self, partition_spec_id: i32) -> super::DataFile {
        let content = match self.content {
            0 => DataContentType::Data,
            1 => DataContentType::PositionDeletes,
            2 => DataContentType::EqualityDeletes,
            _ => DataContentType::Data,
        };
        let file_format = match self.file_format.as_str() {
            "PARQUET" => DataFileFormat::Parquet,
            "AVRO" => DataFileFormat::Avro,
            "ORC" => DataFileFormat::Orc,
            _ => DataFileFormat::Parquet,
        };
        super::DataFile {
            content,
            file_path: self.file_path,
            file_format,
            partition: Vec::new(),
            record_count: self.record_count as u64,
            file_size_in_bytes: self.file_size_in_bytes as u64,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            block_size_in_bytes: None,
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets.unwrap_or_default(),
            equality_ids: self.equality_ids.unwrap_or_default(),
            sort_order_id: self.sort_order_id,
            first_row_id: self.first_row_id,
            partition_spec_id,
            referenced_data_file: self.referenced_data_file,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
        }
    }
}
