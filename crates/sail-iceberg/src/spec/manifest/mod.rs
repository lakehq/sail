use std::sync::Arc;

use apache_avro::{from_value as avro_from_value, Reader as AvroReader};

use super::Schema;

mod _serde;
mod data_file;
mod entry;
mod metadata;

pub use data_file::*;
pub use entry::*;
pub use metadata::*;

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

        // Determine partition type to guide value decoding when needed
        let partition_type = metadata
            .partition_spec
            .partition_type(&metadata.schema)
            .map_err(|e| format!("Partition type error: {e}"))?;

        // For entries, reuse the embedded schema in the Avro file and deserialize per record
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
        let mut entries = Vec::new();
        for value in reader {
            let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
            let entry_avro: _serde::ManifestEntryAvro =
                avro_from_value(&value).map_err(|e| format!("Avro decode entry error: {e}"))?;
            let data_file = entry_avro.data_file.into_data_file(
                &metadata.schema,
                partition_type.fields().len() as i32,
                metadata.partition_spec.spec_id(),
            );
            let status = match entry_avro.status {
                1 => ManifestStatus::Added,
                2 => ManifestStatus::Deleted,
                _ => ManifestStatus::Existing,
            };
            let entry = ManifestEntry::new(
                status,
                entry_avro.snapshot_id,
                entry_avro.sequence_number,
                entry_avro.file_sequence_number,
                data_file,
            );
            entries.push(entry);
        }

        Ok((metadata, entries))
    }

    /// Parse a manifest from bytes of avro file.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, String> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Manifest::new(metadata, entries))
    }
}

// Helper functions used by Avro serde to parse partition values and bounds
use crate::spec::datatypes::Type;
use crate::spec::values::Literal;
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType};

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

fn parse_primitive_bound(bytes: &[u8], prim_type: &PrimitiveType) -> Result<Datum, String> {
    use num_bigint::BigInt;
    use num_traits::ToPrimitive;

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
