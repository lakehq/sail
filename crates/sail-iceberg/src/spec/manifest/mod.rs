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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest/mod.rs

use std::sync::Arc;

use apache_avro::{from_value as avro_from_value, Reader as AvroReader};

use super::Schema;

mod _serde;
mod data_file;
mod entry;
mod metadata;
mod schema;
mod writer;

// Provide data file avro helpers API surface
use apache_avro::{to_value, Writer as AvroWriter};
pub use data_file::*;
pub use entry::*;
pub use metadata::*;
pub use writer::*;

use crate::spec::metadata::format::FormatVersion;
use crate::spec::types::StructType;
use crate::spec::Schema as IcebergSchema;

/// Convert data files to avro bytes and write to writer. Return the bytes written.
pub fn write_data_files_to_avro<W: std::io::Write>(
    writer: &mut W,
    data_files: impl IntoIterator<Item = DataFile>,
    partition_type: &StructType,
    version: FormatVersion,
) -> Result<usize, String> {
    let avro_schema = match version {
        FormatVersion::V1 => super::manifest::schema::data_file_schema_v2(partition_type),
        FormatVersion::V2 => super::manifest::schema::data_file_schema_v2(partition_type),
    };
    let mut writer = AvroWriter::new(&avro_schema, writer);

    for data_file in data_files {
        let serde_df = super::manifest::_serde::DataFileSerde::from_data_file(data_file);
        let value = to_value(serde_df)
            .map_err(|e| format!("Avro to_value error: {e}"))?
            .resolve(&avro_schema)
            .map_err(|e| format!("Avro resolve error: {e}"))?;
        writer
            .append(value)
            .map_err(|e| format!("Avro append error: {e}"))?;
    }

    writer.flush().map_err(|e| format!("Avro flush error: {e}"))
}

/// Parse data files from avro bytes.
pub fn read_data_files_from_avro<R: std::io::Read>(
    reader: &mut R,
    _schema: &IcebergSchema,
    partition_spec_id: i32,
    partition_type: &StructType,
    _version: FormatVersion,
) -> Result<Vec<DataFile>, String> {
    let avro_schema = super::manifest::schema::data_file_schema_v2(partition_type);
    let reader = AvroReader::with_schema(&avro_schema, reader)
        .map_err(|e| format!("Avro reader error: {e}"))?;
    reader
        .into_iter()
        .map(|value| {
            let value = value.map_err(|e| format!("Avro read error: {e}"))?;
            let serde_df: super::manifest::_serde::DataFileSerde =
                avro_from_value(&value).map_err(|e| format!("Avro decode DataFile error: {e}"))?;
            let df = DataFile {
                content: match serde_df.content {
                    0 => DataContentType::Data,
                    1 => DataContentType::PositionDeletes,
                    2 => DataContentType::EqualityDeletes,
                    _ => DataContentType::Data,
                },
                file_path: serde_df.file_path,
                file_format: match serde_df.file_format.as_str() {
                    "PARQUET" => DataFileFormat::Parquet,
                    "AVRO" => DataFileFormat::Avro,
                    "ORC" => DataFileFormat::Orc,
                    _ => DataFileFormat::Parquet,
                },
                partition: Vec::new(),
                record_count: serde_df.record_count as u64,
                file_size_in_bytes: serde_df.file_size_in_bytes as u64,
                column_sizes: Default::default(),
                value_counts: Default::default(),
                null_value_counts: Default::default(),
                nan_value_counts: Default::default(),
                lower_bounds: Default::default(),
                upper_bounds: Default::default(),
                block_size_in_bytes: None,
                key_metadata: serde_df.key_metadata,
                split_offsets: serde_df.split_offsets.unwrap_or_default(),
                equality_ids: serde_df.equality_ids.unwrap_or_default(),
                sort_order_id: serde_df.sort_order_id,
                first_row_id: serde_df.first_row_id,
                partition_spec_id,
                referenced_data_file: serde_df.referenced_data_file,
                content_offset: serde_df.content_offset,
                content_size_in_bytes: serde_df.content_size_in_bytes,
            };
            Ok(df)
        })
        .collect::<Result<Vec<_>, String>>()
}

/// Reference to [`ManifestEntry`].
pub type ManifestEntryRef = Arc<ManifestEntry>;

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Manifest {
    /// Metadata about the manifest.
    pub metadata: ManifestMetadata,
    /// Entries in the manifest.
    pub entries: Vec<ManifestEntryRef>,
}

impl Manifest {
    /// Create a new manifest.
    pub fn new(metadata: ManifestMetadata, entries: Vec<ManifestEntry>) -> Self {
        Self {
            metadata,
            entries: entries.into_iter().map(Arc::new).collect(),
        }
    }

    /// Get the entries in the manifest.
    pub fn entries(&self) -> &[ManifestEntryRef] {
        &self.entries
    }

    /// Get the metadata of the manifest.
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }

    /// Consume this Manifest, returning its constituent parts
    pub fn into_parts(self) -> (Vec<ManifestEntryRef>, ManifestMetadata) {
        let Self { entries, metadata } = self;
        (entries, metadata)
    }

    /// Parse manifest metadata and entries from bytes of avro file.
    pub(crate) fn try_from_avro_bytes(
        bs: &[u8],
    ) -> Result<(ManifestMetadata, Vec<ManifestEntry>), String> {
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;

        // Parse manifest metadata from avro user metadata
        let meta = reader.user_metadata();
        let metadata = ManifestMetadata::parse_from_avro_meta(meta)?;

        // For entries, use typed serde model
        let mut entries = Vec::new();
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
        for value in reader {
            let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
            let entry: _serde::ManifestEntryV2 =
                avro_from_value(&value).map_err(|e| format!("Avro decode entry error: {e}"))?;
            entries.push(entry.into_entry(metadata.partition_spec.spec_id()));
        }

        Ok((metadata, entries))
    }

    /// Parse a manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, String> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Manifest::new(metadata, entries))
    }

    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        let builder = crate::spec::manifest::writer::ManifestWriterBuilder::new(
            None,
            None,
            self.metadata.clone(),
        );
        let mut w = builder.build();
        for e in &self.entries {
            w.add(e.data_file.clone());
        }
        w.to_avro_bytes_v2()
    }
}

// Helper functions used by Avro serde to parse partition values and bounds
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::Type;
use crate::spec::{Datum, PrimitiveType};

#[allow(dead_code)]
pub(super) fn parse_partition_values(json: Option<&serde_json::Value>) -> Vec<Option<Literal>> {
    match json {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| match v {
                serde_json::Value::Null => None,
                serde_json::Value::Bool(b) => {
                    Some(Literal::Primitive(PrimitiveLiteral::Boolean(*b)))
                }
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                            Some(Literal::Primitive(PrimitiveLiteral::Int(i as i32)))
                        } else {
                            Some(Literal::Primitive(PrimitiveLiteral::Long(i)))
                        }
                    } else {
                        n.as_f64().map(|f| {
                            Literal::Primitive(PrimitiveLiteral::Double(
                                ordered_float::OrderedFloat(f),
                            ))
                        })
                    }
                }
                serde_json::Value::String(s) => {
                    Some(Literal::Primitive(PrimitiveLiteral::String(s.clone())))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[allow(dead_code)]
pub(super) fn parse_i64_map_from_avro(
    values: &Option<apache_avro::types::Value>,
) -> std::collections::HashMap<i32, i64> {
    use apache_avro::types::Value;
    let mut map = std::collections::HashMap::new();
    if let Some(Value::Map(obj)) = values {
        for (k, v) in obj {
            if let Value::Long(i) = v {
                map.insert(k.parse::<i32>().unwrap_or(0), *i);
            }
        }
        return map;
    }
    if let Some(Value::Array(vec)) = values {
        for item in vec {
            if let Value::Record(fields) = item {
                let mut key_opt = None;
                let mut value_opt = None;
                for (name, val) in fields {
                    match name.as_str() {
                        "key" => {
                            if let Value::Int(k) = val {
                                key_opt = Some(*k);
                            }
                        }
                        "value" => {
                            if let Value::Long(vl) = val {
                                value_opt = Some(*vl);
                            }
                        }
                        _ => {}
                    }
                }
                if let (Some(k), Some(v)) = (key_opt, value_opt) {
                    map.insert(k, v);
                }
            }
        }
    }
    map
}

#[allow(dead_code)]
pub(super) fn parse_bytes_map_from_avro(
    values: &Option<apache_avro::types::Value>,
) -> Option<std::collections::HashMap<i32, Vec<u8>>> {
    use apache_avro::types::Value;
    if let Some(Value::Map(obj)) = values {
        let mut map = std::collections::HashMap::new();
        for (k, v) in obj {
            if let Value::Bytes(b) = v {
                map.insert(k.parse::<i32>().unwrap_or(0), b.clone());
            }
        }
        return Some(map);
    }
    if let Some(Value::Array(vec)) = values {
        let mut map = std::collections::HashMap::new();
        for item in vec {
            if let Value::Record(fields) = item {
                let mut key_opt = None;
                let mut value_opt = None;
                for (name, val) in fields {
                    match name.as_str() {
                        "key" => {
                            if let Value::Int(k) = val {
                                key_opt = Some(*k);
                            }
                        }
                        "value" => {
                            if let Value::Bytes(b) = val {
                                value_opt = Some(b.clone());
                            }
                        }
                        _ => {}
                    }
                }
                if let (Some(k), Some(v)) = (key_opt, value_opt) {
                    map.insert(k, v);
                }
            }
        }
        return Some(map);
    }
    None
}

#[allow(dead_code)]
pub(super) fn parse_bounds_from_binary(
    bounds_data: Option<&std::collections::HashMap<i32, Vec<u8>>>,
    schema: &Schema,
) -> std::collections::HashMap<i32, Datum> {
    let mut bounds = std::collections::HashMap::new();
    if let Some(data) = bounds_data {
        for (field_id, binary_data) in data {
            if let Some(field) = schema.field_by_id(*field_id) {
                let field_type = field.field_type.as_ref();
                let datum = match field_type {
                    Type::Primitive(prim_type) => {
                        parse_primitive_bound(binary_data, prim_type).ok()
                    }
                    _ => None,
                };
                if let Some(d) = datum {
                    bounds.insert(*field_id, d);
                }
            } else if let Ok(string_value) = String::from_utf8(binary_data.clone()) {
                bounds.insert(
                    *field_id,
                    Datum::new(
                        PrimitiveType::String,
                        PrimitiveLiteral::String(string_value),
                    ),
                );
            } else {
                bounds.insert(
                    *field_id,
                    Datum::new(
                        PrimitiveType::Binary,
                        PrimitiveLiteral::Binary(binary_data.clone()),
                    ),
                );
            }
        }
    }
    bounds
}

#[allow(dead_code)]
fn parse_primitive_bound(bytes: &[u8], prim_type: &PrimitiveType) -> Result<Datum, String> {
    use num_bigint::BigInt;
    use rust_decimal::prelude::ToPrimitive;

    let literal = match prim_type {
        PrimitiveType::Boolean => {
            let val = !(bytes.len() == 1 && bytes[0] == 0u8);
            PrimitiveLiteral::Boolean(val)
        }
        PrimitiveType::Int | PrimitiveType::Date => {
            let val = i32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i32 bytes")?);
            PrimitiveLiteral::Int(val)
        }
        PrimitiveType::Long
        | PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz
        | PrimitiveType::TimestampNs
        | PrimitiveType::TimestamptzNs => {
            let val = if bytes.len() == 4 {
                i32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i32 bytes")?) as i64
            } else {
                i64::from_le_bytes(bytes.try_into().map_err(|_| "Invalid i64 bytes")?)
            };
            PrimitiveLiteral::Long(val)
        }
        PrimitiveType::Float => {
            let val = f32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f32 bytes")?);
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(val))
        }
        PrimitiveType::Double => {
            let val = if bytes.len() == 4 {
                f32::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f32 bytes")?) as f64
            } else {
                f64::from_le_bytes(bytes.try_into().map_err(|_| "Invalid f64 bytes")?)
            };
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(val))
        }
        PrimitiveType::String => {
            let val = std::str::from_utf8(bytes)
                .map_err(|_| "Invalid UTF-8")?
                .to_string();
            PrimitiveLiteral::String(val)
        }
        PrimitiveType::Uuid => {
            let val = u128::from_be_bytes(bytes.try_into().map_err(|_| "Invalid UUID bytes")?);
            PrimitiveLiteral::UInt128(val)
        }
        PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
            PrimitiveLiteral::Binary(Vec::from(bytes))
        }
        PrimitiveType::Decimal { .. } => {
            let unscaled_value = BigInt::from_signed_bytes_be(bytes);
            let val = unscaled_value
                .to_i128()
                .ok_or_else(|| format!("Can't convert bytes to i128: {:?}", bytes))?;
            PrimitiveLiteral::Int128(val)
        }
    };
    Ok(Datum::new(prim_type.clone(), literal))
}
