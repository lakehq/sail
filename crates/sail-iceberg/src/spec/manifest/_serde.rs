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

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::ByteBuf;

use super::{DataContentType, DataFileFormat};
use crate::spec::Schema;
use crate::spec::types::{Datum, RawLiteral, StructType, Type};

#[derive(Serialize, Deserialize)]
pub(super) struct IntLongMapEntry {
    key: i32,
    value: i64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct IntBytesMapEntry {
    key: i32,
    value: ByteBuf,
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
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OptionalI64Serde {
    Some(i64),
    Optional(Option<i64>),
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
    })
}

fn deserialize_optional_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    OptionalI64Serde::deserialize(deserializer).map(|value| match value {
        OptionalI64Serde::Some(value) => Some(value),
        OptionalI64Serde::Optional(value) => value,
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
    #[serde(default, deserialize_with = "deserialize_optional_i64")]
    pub snapshot_id: Option<i64>,
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
        deserialize_with = "deserialize_optional_i64"
    )]
    pub block_size_in_bytes: Option<i64>,
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
    pub key_metadata: Option<ByteBuf>,
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

impl TryFrom<i32> for super::ManifestStatus {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Existing),
            1 => Ok(Self::Added),
            2 => Ok(Self::Deleted),
            _ => Err(format!("Invalid Iceberg manifest entry status: {value}")),
        }
    }
}

impl TryFrom<i32> for DataContentType {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::PositionDeletes),
            2 => Ok(Self::EqualityDeletes),
            _ => Err(format!("Invalid Iceberg data file content: {value}")),
        }
    }
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
        let status = super::ManifestStatus::try_from(self.status)?;
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

fn int_long_map_from(
    name: &str,
    values: HashMap<i32, u64>,
) -> Result<Option<Vec<IntLongMapEntry>>, String> {
    if values.is_empty() {
        Ok(None)
    } else {
        let mut out = values
            .into_iter()
            .map(|(key, value)| {
                Ok(IntLongMapEntry {
                    key,
                    value: i64::try_from(value).map_err(|_| {
                        format!(
                            "Iceberg data file metric `{name}` field id {key} exceeds i64: {value}"
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        out.sort_by_key(|entry| entry.key);
        Ok(Some(out))
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
            let value = u64::try_from(entry.value).map_err(|_| {
                format!(
                    "Invalid negative value {} for Iceberg data file metric `{name}` field id {}",
                    entry.value, entry.key
                )
            })?;
            Ok((entry.key, value))
        })
        .collect()
}

fn bytes_map_from(values: HashMap<i32, Datum>) -> Result<Option<Vec<IntBytesMapEntry>>, String> {
    if values.is_empty() {
        Ok(None)
    } else {
        let mut out = values
            .into_iter()
            .map(|(key, datum)| {
                Ok(IntBytesMapEntry {
                    key,
                    value: ByteBuf::from(datum.to_bytes()?),
                })
            })
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
                .literal_from_bytes(entry.value.as_ref())
                .ok()
                .map(|literal| (entry.key, Datum::new(primitive, literal)))
        })
        .collect()
}

impl ManifestEntryV1 {
    pub fn from_entry(
        entry: super::ManifestEntry,
        partition_type: &StructType,
    ) -> Result<Self, String> {
        let mut data_file = DataFileSerde::from_data_file(entry.data_file, partition_type)?;
        data_file.block_size_in_bytes = Some(data_file.block_size_in_bytes.unwrap_or(67_108_864));
        Ok(Self {
            status: match entry.status {
                super::ManifestStatus::Added => 1,
                super::ManifestStatus::Deleted => 2,
                super::ManifestStatus::Existing => 0,
            },
            snapshot_id: entry.snapshot_id,
            data_file,
        })
    }

    pub fn into_entry(
        self,
        partition_spec_id: i32,
        partition_type: &StructType,
        schema: Option<&Schema>,
    ) -> Result<super::ManifestEntry, String> {
        let status = super::ManifestStatus::try_from(self.status)?;
        Ok(super::ManifestEntry::new(
            status,
            self.snapshot_id,
            None,
            None,
            self.data_file
                .into_data_file(partition_spec_id, partition_type, schema)?,
        ))
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
            record_count: i64::try_from(df.record_count)
                .map_err(|_| format!("Iceberg record_count exceeds i64: {}", df.record_count))?,
            file_size_in_bytes: i64::try_from(df.file_size_in_bytes).map_err(|_| {
                format!(
                    "Iceberg file_size_in_bytes exceeds i64: {}",
                    df.file_size_in_bytes
                )
            })?,
            block_size_in_bytes: df.block_size_in_bytes,
            column_sizes: int_long_map_from("column_sizes", df.column_sizes)?,
            value_counts: int_long_map_from("value_counts", df.value_counts)?,
            null_value_counts: int_long_map_from("null_value_counts", df.null_value_counts)?,
            nan_value_counts: int_long_map_from("nan_value_counts", df.nan_value_counts)?,
            lower_bounds: bytes_map_from(df.lower_bounds)?,
            upper_bounds: bytes_map_from(df.upper_bounds)?,
            key_metadata: df.key_metadata.map(ByteBuf::from),
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
        let content = DataContentType::try_from(self.content)?;
        let file_format = match self.file_format.to_ascii_uppercase().as_str() {
            "PARQUET" => DataFileFormat::Parquet,
            "AVRO" => DataFileFormat::Avro,
            "ORC" => DataFileFormat::Orc,
            "PUFFIN" => DataFileFormat::Puffin,
            _ => return Err(format!("Invalid Iceberg file format: {}", self.file_format)),
        };
        let partition = match self.partition {
            Some(partition) => partition.into_struct_values(partition_type)?,
            // Preserve the legacy empty-tuple fallback only for unpartitioned files.
            None if partition_type.fields().is_empty() => Vec::new(),
            None => return Err("Missing partition tuple for partitioned Iceberg data file".into()),
        };
        Ok(super::DataFile {
            content,
            file_path: self.file_path,
            file_format,
            partition,
            record_count: u64::try_from(self.record_count).map_err(|_| {
                format!(
                    "Invalid negative Iceberg record_count: {}",
                    self.record_count
                )
            })?,
            file_size_in_bytes: u64::try_from(self.file_size_in_bytes).map_err(|_| {
                format!(
                    "Invalid negative Iceberg file_size_in_bytes: {}",
                    self.file_size_in_bytes
                )
            })?,
            column_sizes: int_long_map_into("column_sizes", self.column_sizes)?,
            value_counts: int_long_map_into("value_counts", self.value_counts)?,
            null_value_counts: int_long_map_into("null_value_counts", self.null_value_counts)?,
            nan_value_counts: int_long_map_into("nan_value_counts", self.nan_value_counts)?,
            lower_bounds: bytes_map_into(self.lower_bounds, schema),
            upper_bounds: bytes_map_into(self.upper_bounds, schema),
            block_size_in_bytes: self.block_size_in_bytes,
            key_metadata: self.key_metadata.map(ByteBuf::into_vec),
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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use apache_avro::types::Value as AvroValue;
    use serde_json::json;

    use super::*;
    use crate::spec::types::{Literal, NestedField, PrimitiveLiteral, PrimitiveType};

    fn data_file() -> DataFileSerde {
        serde_json::from_value(json!({
            "file_path": "data.parquet",
            "file_format": "PARQUET",
            "record_count": 1,
            "file_size_in_bytes": 20
        }))
        .expect("data file fixture")
    }

    fn unpartitioned() -> StructType {
        StructType::new(vec![])
    }

    #[test]
    #[expect(clippy::panic)]
    fn malformed_optional_collections_are_rejected() {
        for name in [
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
            "split_offsets",
            "equality_ids",
        ] {
            for invalid in [
                AvroValue::String("invalid".into()),
                AvroValue::Array(vec![AvroValue::String("invalid element".into())]),
            ] {
                for value in [invalid.clone(), AvroValue::Union(1, Box::new(invalid))] {
                    let AvroValue::Record(mut fields) =
                        apache_avro::to_value(data_file()).expect("data file fixture")
                    else {
                        panic!("expected record");
                    };
                    fields.retain(|(field, _)| field != name);
                    fields.push((name.into(), value));
                    assert!(
                        apache_avro::from_value::<DataFileSerde>(&AvroValue::Record(fields))
                            .is_err(),
                        "malformed {name} must not become absent"
                    );
                }
            }
        }
    }

    #[test]
    fn entry_status_rejects_unknown_values_in_both_versions() {
        for status in [-1, 3, i32::MAX] {
            let v1 = ManifestEntryV1 {
                status,
                snapshot_id: Some(1),
                data_file: data_file(),
            };
            assert!(v1.into_entry(0, &unpartitioned(), None).is_err());
            let v2 = ManifestEntryV2 {
                status,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                data_file: data_file(),
            };
            assert!(v2.into_entry(0, &unpartitioned(), None).is_err());
        }
        for (status, expected) in [
            (0, super::super::ManifestStatus::Existing),
            (1, super::super::ManifestStatus::Added),
            (2, super::super::ManifestStatus::Deleted),
        ] {
            let v1 = ManifestEntryV1 {
                status,
                snapshot_id: Some(1),
                data_file: data_file(),
            };
            assert_eq!(
                v1.into_entry(0, &unpartitioned(), None)
                    .expect("valid status")
                    .status,
                expected
            );
            let v2 = ManifestEntryV2 {
                status,
                snapshot_id: Some(1),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                data_file: data_file(),
            };
            assert_eq!(
                v2.into_entry(0, &unpartitioned(), None)
                    .expect("valid status")
                    .status,
                expected
            );
        }
    }

    #[test]
    fn data_file_rejects_unknown_content_and_format() {
        for content in [-1, 3, i32::MAX] {
            let mut file = data_file();
            file.content = content;
            assert!(file.into_data_file(0, &unpartitioned(), None).is_err());
        }
        for (content, expected) in [
            (0, DataContentType::Data),
            (1, DataContentType::PositionDeletes),
            (2, DataContentType::EqualityDeletes),
        ] {
            let mut file = data_file();
            file.content = content;
            assert_eq!(
                file.into_data_file(0, &unpartitioned(), None)
                    .expect("valid content")
                    .content,
                expected
            );
        }
        let mut file = data_file();
        file.file_format = "unknown".into();
        assert!(file.into_data_file(0, &unpartitioned(), None).is_err());
        // Apache Iceberg FileFormat.fromString accepts either case.
        for (format, expected) in [
            ("avro", DataFileFormat::Avro),
            ("Orc", DataFileFormat::Orc),
            ("PARQUET", DataFileFormat::Parquet),
            ("puffin", DataFileFormat::Puffin),
        ] {
            let mut file = data_file();
            file.file_format = format.into();
            assert_eq!(
                file.into_data_file(0, &unpartitioned(), None)
                    .expect("valid format")
                    .file_format,
                expected
            );
        }
    }

    #[test]
    fn counts_and_sizes_reject_negative_input_and_output_overflow() {
        for record_count in [true, false] {
            let mut wire = data_file();
            if record_count {
                wire.record_count = -1;
            } else {
                wire.file_size_in_bytes = -1;
            }
            assert!(wire.into_data_file(0, &unpartitioned(), None).is_err());
            let mut file = data_file()
                .into_data_file(0, &unpartitioned(), None)
                .expect("valid file");
            if record_count {
                file.record_count = u64::MAX;
            } else {
                file.file_size_in_bytes = u64::MAX;
            }
            assert!(DataFileSerde::from_data_file(file, &unpartitioned()).is_err());
        }
        let mut wire = data_file();
        wire.record_count = i64::MAX;
        wire.file_size_in_bytes = 0;
        let file = wire
            .into_data_file(0, &unpartitioned(), None)
            .expect("valid boundaries");
        let wire = DataFileSerde::from_data_file(file, &unpartitioned()).expect("valid boundaries");
        assert_eq!(wire.record_count, i64::MAX);
        assert_eq!(wire.file_size_in_bytes, 0);
    }

    #[test]
    fn metrics_reject_negative_input_and_output_overflow() {
        for name in [
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
        ] {
            assert!(
                int_long_map_into(name, Some(vec![IntLongMapEntry { key: 1, value: -1 }])).is_err()
            );
            assert!(int_long_map_from(name, HashMap::from([(1, u64::MAX)])).is_err());
            let values = HashMap::from([(1, 0), (2, i64::MAX as u64)]);
            let encoded = int_long_map_from(name, values.clone()).expect("encode metrics");
            assert_eq!(
                int_long_map_into(name, encoded).expect("decode metrics"),
                values
            );

            let mut wire = data_file();
            let invalid = Some(vec![IntLongMapEntry { key: 1, value: -1 }]);
            match name {
                "column_sizes" => wire.column_sizes = invalid,
                "value_counts" => wire.value_counts = invalid,
                "null_value_counts" => wire.null_value_counts = invalid,
                _ => wire.nan_value_counts = invalid,
            }
            assert!(wire.into_data_file(0, &unpartitioned(), None).is_err());
            let mut file = data_file()
                .into_data_file(0, &unpartitioned(), None)
                .expect("valid file");
            let invalid = HashMap::from([(1, u64::MAX)]);
            match name {
                "column_sizes" => file.column_sizes = invalid,
                "value_counts" => file.value_counts = invalid,
                "null_value_counts" => file.null_value_counts = invalid,
                _ => file.nan_value_counts = invalid,
            }
            assert!(DataFileSerde::from_data_file(file, &unpartitioned()).is_err());
        }
    }

    #[test]
    fn partitioned_files_require_a_tuple_but_accept_legacy_nullable_records() {
        let partition_type = StructType::new(vec![
            NestedField::optional(1000, "p", Type::Primitive(PrimitiveType::Int)).into(),
        ]);
        assert!(
            data_file()
                .into_data_file(0, &partition_type, None)
                .is_err()
        );
        assert!(
            data_file()
                .into_data_file(0, &unpartitioned(), None)
                .expect("legacy unpartitioned file")
                .partition
                .is_empty()
        );
        for nullable in [false, true] {
            let value = AvroValue::Record(vec![(
                "p".into(),
                AvroValue::Union(1, Box::new(AvroValue::Int(7))),
            )]);
            let value = if nullable {
                AvroValue::Union(1, Box::new(value))
            } else {
                value
            };
            #[derive(Deserialize)]
            struct Partition {
                #[serde(deserialize_with = "deserialize_optional_raw_literal")]
                partition: Option<RawLiteral>,
            }
            let decoded = apache_avro::from_value::<Partition>(&AvroValue::Record(vec![(
                "partition".into(),
                value,
            )]))
            .expect("legacy partition record");
            let mut file = data_file();
            file.partition = decoded.partition;
            assert_eq!(
                file.into_data_file(0, &partition_type, None)
                    .expect("partitioned file")
                    .partition,
                vec![Some(Literal::Primitive(PrimitiveLiteral::Int(7)))]
            );
        }
    }
}
