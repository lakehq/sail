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

use std::collections::HashMap;

use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};

use super::{DataContentType, DataFileFormat};
use crate::spec::types::{Datum, PrimitiveLiteral, RawLiteral, StructType, Type};
use crate::spec::Schema;

#[derive(Serialize, Deserialize)]
pub(super) struct IntLongMapEntry {
    key: i32,
    value: i64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct IntBytesMapEntry {
    key: i32,
    value: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OptionalRawLiteralSerde {
    Some(RawLiteral),
    Optional(Option<RawLiteral>),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OptionalVecSerde<T> {
    Some(Vec<T>),
    Optional(Option<Vec<T>>),
    Other(IgnoredAny),
}

fn deserialize_optional_raw_literal<'de, D>(deserializer: D) -> Result<Option<RawLiteral>, D::Error>
where
    D: Deserializer<'de>,
{
    OptionalRawLiteralSerde::deserialize(deserializer).map(|value| match value {
        OptionalRawLiteralSerde::Some(value) => Some(value),
        OptionalRawLiteralSerde::Optional(value) => value,
    })
}

fn deserialize_optional_vec<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    OptionalVecSerde::<T>::deserialize(deserializer).map(|value| match value {
        OptionalVecSerde::Some(value) => Some(value),
        OptionalVecSerde::Optional(value) => value,
        OptionalVecSerde::Other(_) => None,
    })
}

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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_raw_literal"
    )]
    pub partition: Option<RawLiteral>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub column_sizes: Option<Vec<IntLongMapEntry>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub value_counts: Option<Vec<IntLongMapEntry>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub null_value_counts: Option<Vec<IntLongMapEntry>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub nan_value_counts: Option<Vec<IntLongMapEntry>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub lower_bounds: Option<Vec<IntBytesMapEntry>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub upper_bounds: Option<Vec<IntBytesMapEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_vec"
    )]
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
    pub fn from_entry(
        entry: super::ManifestEntry,
        partition_type: &StructType,
    ) -> Result<Self, String> {
        Ok(Self {
            status: match entry.status {
                super::ManifestStatus::Added => 1,
                super::ManifestStatus::Deleted => 2,
                super::ManifestStatus::Existing => 0,
            },
            snapshot_id: entry.snapshot_id,
            sequence_number: entry.sequence_number,
            file_sequence_number: entry.file_sequence_number,
            data_file: DataFileSerde::from_data_file(entry.data_file, partition_type)?,
        })
    }

    pub fn into_entry(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: Option<&Schema>,
    ) -> Result<super::ManifestEntry, String> {
        let status = match self.status {
            1 => super::ManifestStatus::Added,
            2 => super::ManifestStatus::Deleted,
            _ => super::ManifestStatus::Existing,
        };
        Ok(super::ManifestEntry::new(
            status,
            self.snapshot_id,
            self.sequence_number,
            self.file_sequence_number,
            self.data_file
                .into_data_file(partition_spec_id, partition_type, schema)?,
        ))
    }
}

fn int_long_map_from(values: HashMap<i32, u64>) -> Option<Vec<IntLongMapEntry>> {
    if values.is_empty() {
        None
    } else {
        let mut out = values
            .into_iter()
            .map(|(key, value)| IntLongMapEntry {
                key,
                value: value as i64,
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|entry| entry.key);
        Some(out)
    }
}

fn int_long_map_into(
    name: &str,
    values: Option<Vec<IntLongMapEntry>>,
) -> Result<HashMap<i32, u64>, String> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|entry| {
            if entry.value < 0 {
                Err(format!(
                    "Invalid negative value {} for Iceberg data file metric `{name}` field id {}",
                    entry.value, entry.key
                ))
            } else {
                Ok((entry.key, entry.value as u64))
            }
        })
        .collect()
}

fn i128_to_min_big_endian(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let current = bytes[start];
        let next = bytes[start + 1];
        let redundant_positive = current == 0x00 && (next & 0x80) == 0;
        let redundant_negative = current == 0xff && (next & 0x80) != 0;
        if redundant_positive || redundant_negative {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

fn datum_to_bytes(datum: &Datum) -> Result<Vec<u8>, String> {
    let bytes = match &datum.literal {
        PrimitiveLiteral::Boolean(value) => vec![u8::from(*value)],
        PrimitiveLiteral::Int(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Long(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Float(value) => value.0.to_le_bytes().to_vec(),
        PrimitiveLiteral::Double(value) => value.0.to_le_bytes().to_vec(),
        PrimitiveLiteral::Int128(value) => i128_to_min_big_endian(*value),
        PrimitiveLiteral::String(value) => value.as_bytes().to_vec(),
        PrimitiveLiteral::UInt128(value) => value.to_be_bytes().to_vec(),
        PrimitiveLiteral::Binary(value) => value.clone(),
    };
    if datum.r#type.compatible(&datum.literal) {
        Ok(bytes)
    } else {
        Err(format!(
            "Literal is not compatible with Iceberg type {}",
            datum.r#type
        ))
    }
}

fn bytes_map_from(values: HashMap<i32, Datum>) -> Result<Option<Vec<IntBytesMapEntry>>, String> {
    if values.is_empty() {
        Ok(None)
    } else {
        let mut out = values
            .into_iter()
            .map(|(key, value)| datum_to_bytes(&value).map(|value| IntBytesMapEntry { key, value }))
            .collect::<Result<Vec<_>, String>>()?;
        out.sort_by_key(|entry| entry.key);
        Ok(Some(out))
    }
}

fn bytes_map_into(
    values: Option<Vec<IntBytesMapEntry>>,
    schema: Option<&Schema>,
) -> HashMap<i32, Datum> {
    // TODO: Preserve raw bound bytes like `Map<Integer, ByteBuffer>` metrics.
    // For now, keep only bounds we can decode into `Datum`; unknown fields and unsupported
    // primitive encodings are ignored so manifest reads remain non-fatal after schema evolution.
    values
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let primitive = schema
                .and_then(|schema| schema.field_by_id(entry.key))
                .and_then(|field| match field.field_type.as_ref() {
                    Type::Primitive(primitive) => Some(primitive.clone()),
                    Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
                })?;
            primitive
                .literal_from_bytes(&entry.value)
                .ok()
                .map(|literal| (entry.key, Datum::new(primitive, literal)))
        })
        .collect()
}

#[expect(dead_code)]
impl ManifestEntryV1 {
    pub fn from_entry(
        entry: super::ManifestEntry,
        partition_type: &StructType,
    ) -> Result<Self, String> {
        Ok(Self {
            status: match entry.status {
                super::ManifestStatus::Added => 1,
                super::ManifestStatus::Deleted => 2,
                super::ManifestStatus::Existing => 0,
            },
            snapshot_id: entry.snapshot_id.unwrap_or_default(),
            data_file: DataFileSerde::from_data_file(entry.data_file, partition_type)?,
        })
    }
}

impl DataFileSerde {
    pub fn from_data_file(
        df: super::DataFile,
        partition_type: &StructType,
    ) -> Result<Self, String> {
        Ok(Self {
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
            partition: Some(RawLiteral::from_struct_values(
                &df.partition,
                partition_type,
            )?),
            record_count: df.record_count as i64,
            file_size_in_bytes: df.file_size_in_bytes as i64,
            column_sizes: int_long_map_from(df.column_sizes),
            value_counts: int_long_map_from(df.value_counts),
            null_value_counts: int_long_map_from(df.null_value_counts),
            nan_value_counts: int_long_map_from(df.nan_value_counts),
            lower_bounds: bytes_map_from(df.lower_bounds)?,
            upper_bounds: bytes_map_from(df.upper_bounds)?,
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
        })
    }

    pub fn into_data_file(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: Option<&Schema>,
    ) -> Result<super::DataFile, String> {
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
            "PUFFIN" => DataFileFormat::Puffin,
            _ => DataFileFormat::Parquet,
        };
        Ok(super::DataFile {
            content,
            file_path: self.file_path,
            file_format,
            partition: self
                .partition
                .map(|p| p.into_struct_values(partition_type))
                .unwrap_or_default(),
            record_count: self.record_count as u64,
            file_size_in_bytes: self.file_size_in_bytes as u64,
            column_sizes: int_long_map_into("column_sizes", self.column_sizes)?,
            value_counts: int_long_map_into("value_counts", self.value_counts)?,
            null_value_counts: int_long_map_into("null_value_counts", self.null_value_counts)?,
            nan_value_counts: int_long_map_into("nan_value_counts", self.nan_value_counts)?,
            lower_bounds: bytes_map_into(self.lower_bounds, schema),
            upper_bounds: bytes_map_into(self.upper_bounds, schema),
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
        })
    }
}
