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

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use apache_avro::{
    Reader as AvroReader, Schema as AvroSchema, from_avro_datum, from_value as avro_from_value,
};
use serde_json::Value as JsonValue;

mod _serde;
mod data_file;
mod entry;
mod metadata;
mod schema;
mod writer;

// Provide data file avro helpers API surface
use apache_avro::{Writer as AvroWriter, to_value};
pub use data_file::*;
pub use entry::*;
pub use metadata::*;
pub use writer::*;

use crate::spec::Schema as IcebergSchema;
use crate::spec::metadata::format::FormatVersion;
use crate::spec::types::{PrimitiveType, StructType, Type};

const AVRO_OBJECT_HEADER: &[u8] = b"Obj\x01";

struct AvroObjectHeader {
    metadata: HashMap<String, Vec<u8>>,
    marker: [u8; 16],
    payload_offset: usize,
}

fn parse_avro_object_header(bytes: &[u8]) -> Result<AvroObjectHeader, String> {
    if !bytes.starts_with(AVRO_OBJECT_HEADER) {
        return Err("Invalid Avro object container magic".to_string());
    }
    let mut cursor = std::io::Cursor::new(&bytes[AVRO_OBJECT_HEADER.len()..]);
    let metadata = from_avro_datum(&AvroSchema::map(AvroSchema::Bytes), &mut cursor, None)
        .map_err(|error| format!("Avro object metadata error: {error}"))?;
    let AvroValue::Map(metadata) = metadata else {
        return Err("Avro object metadata is not a map".to_string());
    };
    let metadata = metadata
        .into_iter()
        .map(|(key, value)| match value {
            AvroValue::Bytes(value) => Ok((key, value)),
            _ => Err(format!("Avro object metadata {key:?} is not bytes")),
        })
        .collect::<Result<HashMap<_, _>, String>>()?;
    let mut marker = [0; 16];
    std::io::Read::read_exact(&mut cursor, &mut marker)
        .map_err(|error| format!("Missing Avro object container sync marker: {error}"))?;
    let payload_offset = AVRO_OBJECT_HEADER
        .len()
        .checked_add(
            usize::try_from(cursor.position())
                .map_err(|_| "Avro object header offset exceeds usize".to_string())?,
        )
        .ok_or_else(|| "Avro object header offset overflow".to_string())?;
    Ok(AvroObjectHeader {
        metadata,
        marker,
        payload_offset,
    })
}

fn replace_avro_schema_header_bytes(bytes: &[u8], schema: Vec<u8>) -> Result<Vec<u8>, String> {
    let AvroObjectHeader {
        mut metadata,
        marker,
        payload_offset,
    } = parse_avro_object_header(bytes)?;
    metadata.insert("avro.schema".to_string(), schema);
    let metadata = metadata
        .into_iter()
        .map(|(key, value)| (key, AvroValue::Bytes(value)))
        .collect::<HashMap<_, _>>();
    let encoded_metadata = apache_avro::to_avro_datum(
        &AvroSchema::map(AvroSchema::Bytes),
        AvroValue::Map(metadata),
    )
    .map_err(|error| format!("Avro header serialization error: {error}"))?;

    let mut output = Vec::with_capacity(
        AVRO_OBJECT_HEADER.len() + encoded_metadata.len() + marker.len() + bytes.len()
            - payload_offset,
    );
    output.extend_from_slice(AVRO_OBJECT_HEADER);
    output.extend_from_slice(&encoded_metadata);
    output.extend_from_slice(&marker);
    output.extend_from_slice(&bytes[payload_offset..]);
    Ok(output)
}

pub(super) fn replace_avro_schema_header(
    bytes: &[u8],
    schema: &AvroSchema,
) -> Result<Vec<u8>, String> {
    let schema = serde_json::to_vec(schema)
        .map_err(|error| format!("Avro schema serialization error: {error}"))?;
    replace_avro_schema_header_bytes(bytes, schema)
}

fn find_avro_field_type(schema: &mut JsonValue, field_id: i32) -> Option<&mut JsonValue> {
    match schema {
        JsonValue::Array(branches) => branches
            .iter_mut()
            .find_map(|branch| find_avro_field_type(branch, field_id)),
        JsonValue::Object(object) => {
            let fields = object.get_mut("fields")?.as_array_mut()?;
            for field in fields {
                let is_match = field
                    .get("field-id")
                    .and_then(JsonValue::as_i64)
                    .is_some_and(|id| id == i64::from(field_id));
                if is_match {
                    return field.get_mut("type");
                }
                if let Some(field_type) = field.get_mut("type")
                    && let Some(found) = find_avro_field_type(field_type, field_id)
                {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

fn non_null_avro_schema(schema: &mut JsonValue) -> Option<&mut JsonValue> {
    match schema {
        JsonValue::Array(branches) => branches
            .iter_mut()
            .find(|branch| branch.as_str() != Some("null")),
        schema => Some(schema),
    }
}

fn partition_avro_fields(schema: &mut JsonValue) -> Result<&mut Vec<JsonValue>, String> {
    let partition_schema = find_avro_field_type(schema, 102).ok_or_else(|| {
        "Avro schema is missing the data file partition field (id 102)".to_string()
    })?;
    non_null_avro_schema(partition_schema)
        .and_then(JsonValue::as_object_mut)
        .and_then(|record| record.get_mut("fields"))
        .and_then(JsonValue::as_array_mut)
        .ok_or_else(|| "Avro data file partition field is not a record".to_string())
}

fn fixed_schema_name(schema: &JsonValue) -> Option<(String, String)> {
    let object = schema.as_object()?;
    if object.get("type").and_then(JsonValue::as_str) != Some("fixed") {
        return None;
    }
    let name = object.get("name")?.as_str()?;
    let full_name = if name.contains('.') {
        name.to_string()
    } else if let Some(namespace) = object.get("namespace").and_then(JsonValue::as_str) {
        format!("{namespace}.{name}")
    } else {
        name.to_string()
    };
    Some((name.to_string(), full_name))
}

fn rename_fixed_schema(schema: &mut JsonValue, suffix: usize) -> Result<(), String> {
    let object = schema
        .as_object_mut()
        .ok_or_else(|| "Avro fixed schema must be an object".to_string())?;
    let name = object
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "Avro fixed schema is missing its name".to_string())?;
    let name = match name.rsplit_once('.') {
        Some((namespace, name)) => format!("{namespace}.{name}_sail_{suffix}"),
        None => format!("{name}_sail_{suffix}"),
    };
    object.insert("name".to_string(), JsonValue::String(name));
    Ok(())
}

fn expand_partition_fixed_references(partition_fields: &mut [JsonValue]) -> Result<(), String> {
    let mut definitions = HashMap::<String, JsonValue>::new();
    let mut occurrences = HashMap::<String, usize>::new();
    for avro_field in partition_fields {
        let Some(field_schema) = avro_field.get_mut("type").and_then(non_null_avro_schema) else {
            continue;
        };
        if let Some(reference) = field_schema.as_str() {
            let Some(definition) = definitions.get(reference) else {
                continue;
            };
            let occurrence = occurrences.entry(reference.to_string()).or_insert(1);
            *occurrence += 1;
            *field_schema = definition.clone();
            rename_fixed_schema(field_schema, *occurrence)?;
            continue;
        }

        let Some((name, full_name)) = fixed_schema_name(field_schema) else {
            continue;
        };
        let occurrence = occurrences.entry(full_name.clone()).or_default();
        *occurrence += 1;
        if let Some(definition) = definitions.get(&full_name) {
            if definition != field_schema {
                return Err(format!(
                    "Avro partition schema contains conflicting definitions for {full_name}"
                ));
            }
            rename_fixed_schema(field_schema, *occurrence)?;
        } else {
            definitions.insert(name, field_schema.clone());
            definitions.insert(full_name, field_schema.clone());
        }
    }
    Ok(())
}

fn rewrite_partition_schema(
    schema: &mut JsonValue,
    partition_type: &StructType,
    iceberg_codec: bool,
) -> Result<(), String> {
    let partition_fields = partition_avro_fields(schema)?;
    if iceberg_codec {
        let required_rewrites = partition_type
            .fields()
            .iter()
            .filter(|field| {
                matches!(
                    field.field_type.as_ref(),
                    Type::Primitive(PrimitiveType::Fixed(_) | PrimitiveType::Uuid)
                )
            })
            .count();
        let mut rewritten = 0;
        for avro_field in partition_fields.iter_mut() {
            let Some(field_id) = avro_field
                .get("field-id")
                .and_then(JsonValue::as_i64)
                .and_then(|id| i32::try_from(id).ok())
            else {
                continue;
            };
            let Some(partition_field) = partition_type.field_by_id(field_id) else {
                continue;
            };
            let Type::Primitive(primitive) = partition_field.field_type.as_ref() else {
                continue;
            };
            let Some(field_schema) = avro_field.get_mut("type").and_then(non_null_avro_schema)
            else {
                return Err(format!(
                    "Avro partition field {field_id} is missing its value schema"
                ));
            };
            match primitive {
                PrimitiveType::Fixed(_) => {
                    *field_schema = JsonValue::String("bytes".to_string());
                    rewritten += 1;
                }
                PrimitiveType::Uuid => {
                    if let Some(object) = field_schema.as_object_mut() {
                        object.remove("logicalType");
                    }
                    rewritten += 1;
                }
                _ => {}
            }
        }
        if rewritten != required_rewrites {
            return Err(format!(
                "Avro partition schema contains {rewritten} of {required_rewrites} fixed or UUID fields"
            ));
        }
    }
    expand_partition_fixed_references(partition_fields)
}

fn partition_avro_header(
    bytes: &[u8],
    partition_type: &StructType,
    iceberg_codec: bool,
) -> Result<Vec<u8>, String> {
    let header = parse_avro_object_header(bytes)?;
    let schema = header
        .metadata
        .get("avro.schema")
        .ok_or_else(|| "Avro object header is missing avro.schema metadata".to_string())?;
    let mut schema: JsonValue = serde_json::from_slice(schema)
        .map_err(|error| format!("Avro schema JSON error: {error}"))?;
    rewrite_partition_schema(&mut schema, partition_type, iceberg_codec)?;
    let schema = serde_json::to_vec(&schema)
        .map_err(|error| format!("Avro schema serialization error: {error}"))?;
    replace_avro_schema_header_bytes(bytes, schema)
}

fn decode_avro_values<T, F>(bytes: &[u8], decode: &F) -> Result<Vec<T>, String>
where
    F: Fn(AvroValue) -> Result<T, String>,
{
    AvroReader::new(bytes)
        .map_err(|error| format!("Avro reader error: {error}"))?
        .map(|value| {
            value
                .map_err(|error| format!("Avro read value error: {error}"))
                .and_then(decode)
        })
        .collect()
}

fn decode_iceberg_avro<T, F>(
    bytes: &[u8],
    partition_type: &StructType,
    decode: F,
) -> Result<Vec<T>, String>
where
    F: Fn(AvroValue) -> Result<T, String>,
{
    let (has_named_schema, requires_iceberg_codec) = partition_type.fields().iter().fold(
        (false, false),
        |(has_named_schema, requires_iceberg_codec), field| match field.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Fixed(_) | PrimitiveType::Uuid) => (true, true),
            Type::Primitive(PrimitiveType::Decimal { .. }) => (true, requires_iceberg_codec),
            _ => (has_named_schema, requires_iceberg_codec),
        },
    );
    if !has_named_schema {
        return decode_avro_values(bytes, &decode);
    }

    if !requires_iceberg_codec {
        let normalized_header = partition_avro_header(bytes, partition_type, false)?;
        return decode_avro_values(&normalized_header, &decode);
    }

    let iceberg_result = partition_avro_header(bytes, partition_type, true)
        .and_then(|encoded_header| decode_avro_values(&encoded_header, &decode));
    match iceberg_result {
        Ok(values) => Ok(values),
        Err(iceberg_error) => {
            let standard_result = partition_avro_header(bytes, partition_type, false)
                .and_then(|normalized_header| decode_avro_values(&normalized_header, &decode));
            standard_result.map_err(|standard_error| {
                format!(
                    "Iceberg partition decoding failed ({iceberg_error}); standard Avro fallback also failed ({standard_error})"
                )
            })
        }
    }
}

/// Convert data files to avro bytes and write to writer. Return the bytes written.
pub fn write_data_files_to_avro<W: std::io::Write>(
    writer: &mut W,
    data_files: impl IntoIterator<Item = DataFile>,
    partition_type: &StructType,
    version: FormatVersion,
) -> Result<usize, String> {
    let (declared_schema, encoding_schema) = schema::data_file_schemas(partition_type, version);
    let mut avro_writer = AvroWriter::new(&encoding_schema, Vec::new());

    for data_file in data_files {
        let serde_df = _serde::DataFileSerde::from_data_file(data_file, partition_type)?;
        let value = to_value(serde_df)
            .map_err(|e| format!("Avro to_value error: {e}"))?
            .resolve(&encoding_schema)
            .map_err(|e| format!("Avro resolve error: {e}"))?;
        avro_writer
            .append(value)
            .map_err(|e| format!("Avro append error: {e}"))?;
    }

    let encoded = avro_writer
        .into_inner()
        .map_err(|e| format!("Avro writer finalize error: {e}"))?;
    let output = replace_avro_schema_header(&encoded, &declared_schema)?;
    writer
        .write_all(&output)
        .map_err(|error| format!("Avro output write error: {error}"))?;
    Ok(output.len())
}

/// Parse data files from avro bytes.
pub fn read_data_files_from_avro<R: std::io::Read>(
    reader: &mut R,
    schema: &IcebergSchema,
    partition_spec_id: i32,
    partition_type: &StructType,
    _version: FormatVersion,
) -> Result<Vec<DataFile>, String> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Avro input read error: {error}"))?;
    decode_iceberg_avro(&bytes, partition_type, |value| {
        let serde_df: _serde::DataFileSerde =
            avro_from_value(&value).map_err(|e| format!("Avro decode DataFile error: {e}"))?;
        serde_df.into_data_file(partition_spec_id, partition_type, Some(schema))
    })
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
        // Parse manifest metadata from avro user metadata
        let header = parse_avro_object_header(bs)?;
        let metadata = ManifestMetadata::parse_from_avro_meta(&header.metadata)?;

        // For entries, use typed serde model
        let partition_type = metadata
            .partition_spec
            .partition_type(&metadata.schema)
            .map_err(|e| format!("Partition type error: {e}"))?;
        let entries = decode_iceberg_avro(bs, &partition_type, |value| {
            let entry = match metadata.format_version {
                FormatVersion::V1 => {
                    let entry: _serde::ManifestEntryV1 = avro_from_value(&value)
                        .map_err(|e| format!("Avro decode v1 entry error: {e}"))?;
                    entry.into_entry(
                        metadata.partition_spec.spec_id(),
                        &partition_type,
                        Some(&metadata.schema),
                    )?
                }
                FormatVersion::V2 | FormatVersion::V3 => {
                    let entry: _serde::ManifestEntryV2 = avro_from_value(&value)
                        .map_err(|e| format!("Avro decode entry error: {e}"))?;
                    entry.into_entry(
                        metadata.partition_spec.spec_id(),
                        &partition_type,
                        Some(&metadata.schema),
                    )?
                }
            };
            Ok(entry)
        })?;

        Ok((metadata, entries))
    }

    /// Parse a manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, String> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Manifest::new(metadata, entries))
    }

    pub fn to_avro_bytes_v2(&self) -> Result<Vec<u8>, String> {
        let builder = ManifestWriterBuilder::new(None, None, self.metadata.clone());
        let mut w = builder.build();
        for e in &self.entries {
            w.add_entry(e.as_ref().clone());
        }
        w.to_avro_bytes_v2()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use apache_avro::{Writer as AvroWriter, to_value};
    use serde_json::{Value as JsonValue, json};

    use super::{read_data_files_from_avro, rewrite_partition_schema};
    use crate::spec::types::{NestedField, PrimitiveType, StructType, Type};
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, FormatVersion, Literal, PrimitiveLiteral, Schema,
    };

    #[test]
    fn test_rewrite_partition_schema_preserves_external_record_layout() {
        let partition_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                1000,
                "fixed_value",
                Type::Primitive(PrimitiveType::Fixed(3)),
            )),
            Arc::new(NestedField::optional(
                1001,
                "uuid_value",
                Type::Primitive(PrimitiveType::Uuid),
            )),
            Arc::new(NestedField::optional(
                1002,
                "decimal_value",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            )),
        ]);
        let mut schema = json!({
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {"name": "status", "type": "int", "field-id": 0},
                {
                    "name": "data_file",
                    "field-id": 2,
                    "type": {
                        "type": "record",
                        "name": "r2",
                        "fields": [
                            {
                                "name": "partition",
                                "field-id": 102,
                                "type": {
                                    "type": "record",
                                    "name": "r102",
                                    "fields": [
                                        {
                                            "name": "fixed_value",
                                            "field-id": 1000,
                                            "type": [
                                                "null",
                                                {"type": "fixed", "name": "fixed_3", "size": 3}
                                            ]
                                        },
                                        {
                                            "name": "uuid_value",
                                            "field-id": 1001,
                                            "type": {
                                                "type": "fixed",
                                                "name": "uuid_fixed",
                                                "size": 16,
                                                "logicalType": "uuid"
                                            }
                                        },
                                        {
                                            "name": "decimal_value",
                                            "field-id": 1002,
                                            "type": {
                                                "type": "fixed",
                                                "name": "decimal_9_2",
                                                "size": 4,
                                                "logicalType": "decimal",
                                                "precision": 9,
                                                "scale": 2
                                            }
                                        }
                                    ]
                                }
                            },
                            {"name": "referenced_data_file", "field-id": 143, "type": ["null", "string"]}
                        ]
                    }
                }
            ]
        });

        assert_eq!(
            rewrite_partition_schema(&mut schema, &partition_type, true),
            Ok(())
        );

        assert_eq!(schema.pointer("/fields/1/type/name"), Some(&json!("r2")));
        assert_eq!(
            schema.pointer("/fields/1/type/fields/1/field-id"),
            Some(&json!(143))
        );
        assert_eq!(
            schema.pointer("/fields/1/type/fields/0/type/fields/0/type/1"),
            Some(&json!("bytes"))
        );
        assert_eq!(
            schema.pointer("/fields/1/type/fields/0/type/fields/1/type/type"),
            Some(&json!("fixed"))
        );
        assert!(
            schema
                .pointer("/fields/1/type/fields/0/type/fields/1/type/logicalType")
                .is_none()
        );
        assert_eq!(
            schema.pointer("/fields/1/type/fields/0/type/fields/2/type/logicalType"),
            Some(&json!("decimal"))
        );
    }

    #[test]
    fn test_read_data_files_falls_back_after_typed_fixed_decode_error() -> Result<(), String> {
        let table_schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "first_fixed",
                    Type::Primitive(PrimitiveType::Fixed(3)),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "second_fixed",
                    Type::Primitive(PrimitiveType::Fixed(3)),
                )),
            ])
            .build()?;
        let partition_type = StructType::new(vec![
            Arc::new(NestedField::optional(
                1000,
                "first_fixed",
                Type::Primitive(PrimitiveType::Fixed(3)),
            )),
            Arc::new(NestedField::optional(
                1001,
                "second_fixed",
                Type::Primitive(PrimitiveType::Fixed(3)),
            )),
        ]);
        let expected_partition = vec![
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![4, 5, 6]))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![4, 7, 8]))),
        ];
        let data_file = DataFile {
            content: DataContentType::Data,
            file_path: "data.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: expected_partition.clone(),
            record_count: 1,
            file_size_in_bytes: 1,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let (legacy_schema, _) =
            super::schema::data_file_schemas(&partition_type, FormatVersion::V2);
        let value = to_value(super::_serde::DataFileSerde::from_data_file(
            data_file,
            &partition_type,
        )?)
        .map_err(|error| format!("Avro to_value error: {error}"))?
        .resolve(&legacy_schema)
        .map_err(|error| format!("Avro resolve error: {error}"))?;
        let mut writer = AvroWriter::new(&legacy_schema, Vec::new());
        writer
            .append(value)
            .map_err(|error| format!("Avro append error: {error}"))?;
        let bytes = writer
            .into_inner()
            .map_err(|error| format!("Avro writer finalize error: {error}"))?;
        let header = super::parse_avro_object_header(&bytes)?;
        let mut header_schema: JsonValue = serde_json::from_slice(
            header
                .metadata
                .get("avro.schema")
                .ok_or_else(|| "Avro schema header metadata is missing".to_string())?,
        )
        .map_err(|error| format!("Avro schema JSON error: {error}"))?;
        let partition_fields = super::partition_avro_fields(&mut header_schema)?;
        let (first_field, remaining_fields) = partition_fields
            .split_first_mut()
            .ok_or_else(|| "First partition field is missing".to_string())?;
        let fixed_definition = first_field
            .get_mut("type")
            .and_then(super::non_null_avro_schema)
            .ok_or_else(|| "First fixed schema is missing".to_string())?
            .clone();
        let second_schema = remaining_fields
            .first_mut()
            .and_then(|field| field.get_mut("type"))
            .and_then(super::non_null_avro_schema)
            .ok_or_else(|| "Second fixed schema is missing".to_string())?;
        *second_schema = fixed_definition;
        let header_schema = serde_json::to_vec(&header_schema)
            .map_err(|error| format!("Avro schema serialization error: {error}"))?;
        let bytes = super::replace_avro_schema_header_bytes(&bytes, header_schema)?;

        let parsed = read_data_files_from_avro(
            &mut Cursor::new(bytes),
            &table_schema,
            0,
            &partition_type,
            FormatVersion::V2,
        )?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].partition, expected_partition);
        Ok(())
    }
}
