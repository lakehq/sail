use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use apache_avro::{from_value as avro_from_value, Reader as AvroReader};
use serde::{Deserialize, Serialize};

use super::datatypes::PrimitiveType;
use super::manifest_list::ManifestContentType;
use super::partition::PartitionSpec;
use super::schema::{SchemaId, SchemaRef};
use super::values::{Datum, Literal, PrimitiveLiteral};

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
    ///
    /// TODO: Implement Avro decoding and projection for V1/V2.
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
            let entry_avro: ManifestEntryAvro =
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
    ///
    /// TODO: Implement Avro decoding and projection for V1/V2.
    pub fn parse_avro(bs: &[u8]) -> Result<Self, String> {
        let (metadata, entries) = Self::try_from_avro_bytes(bs)?;
        Ok(Manifest::new(metadata, entries))
    }
}

impl ManifestMetadata {
    pub(crate) fn parse_from_avro_meta(
        meta: &std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<Self, String> {
        // schema
        let schema_bs = meta
            .get("schema")
            .ok_or_else(|| "schema is required in manifest metadata but not found".to_string())?;
        let schema: super::Schema = serde_json::from_slice(schema_bs)
            .map_err(|e| format!("Fail to parse schema in manifest metadata: {e}"))?;
        let schema_ref = std::sync::Arc::new(schema);

        // schema-id (optional)
        let schema_id: i32 = meta
            .get("schema-id")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        // partition-spec and id
        let part_fields_bs = meta.get("partition-spec").ok_or_else(|| {
            "partition-spec is required in manifest metadata but not found".to_string()
        })?;
        let part_fields: Vec<super::partition::PartitionField> =
            serde_json::from_slice(part_fields_bs)
                .map_err(|e| format!("Fail to parse partition spec in manifest metadata: {e}"))?;
        let spec_id: i32 = meta
            .get("partition-spec-id")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        let mut builder = super::partition::PartitionSpec::builder().with_spec_id(spec_id);
        for f in part_fields {
            builder = builder.add_field_with_id(f.source_id, f.field_id, f.name, f.transform);
        }
        let partition_spec = builder.build();

        // format-version
        let format_version = meta
            .get("format-version")
            .and_then(|bs| serde_json::from_slice::<super::FormatVersion>(bs).ok())
            .unwrap_or(super::FormatVersion::V1);

        // content
        let content = meta
            .get("content")
            .and_then(|bs| String::from_utf8(bs.clone()).ok())
            .map(|s| match s.to_ascii_lowercase().as_str() {
                "deletes" => super::manifest_list::ManifestContentType::Deletes,
                _ => super::manifest_list::ManifestContentType::Data,
            })
            .unwrap_or(super::manifest_list::ManifestContentType::Data);

        Ok(ManifestMetadata::new(
            schema_ref,
            schema_id,
            partition_spec,
            format_version,
            content,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestEntryAvro {
    #[serde(rename = "status")]
    status: i32,
    #[serde(rename = "snapshot_id")]
    snapshot_id: Option<i64>,
    #[serde(rename = "sequence_number")]
    sequence_number: Option<i64>,
    #[serde(rename = "file_sequence_number")]
    file_sequence_number: Option<i64>,
    #[serde(rename = "data_file")]
    data_file: DataFileAvro,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataFileAvro {
    #[serde(rename = "content", default)]
    content: i32,
    #[serde(rename = "file_path")]
    file_path: String,
    #[serde(rename = "file_format")]
    file_format: String,
    #[serde(rename = "partition")]
    partition: serde_json::Value,
    #[serde(rename = "record_count")]
    record_count: i64,
    #[serde(rename = "file_size_in_bytes")]
    file_size_in_bytes: i64,
    #[serde(skip)]
    column_sizes: Option<AvroValue>,
    #[serde(skip)]
    value_counts: Option<AvroValue>,
    #[serde(skip)]
    null_value_counts: Option<AvroValue>,
    #[serde(skip)]
    nan_value_counts: Option<AvroValue>,
    #[serde(skip)]
    lower_bounds: Option<AvroValue>,
    #[serde(skip)]
    upper_bounds: Option<AvroValue>,
    #[serde(rename = "key_metadata")]
    key_metadata: Option<Vec<u8>>,
    #[serde(rename = "split_offsets")]
    split_offsets: Option<Vec<i64>>,
    #[serde(rename = "equality_ids")]
    equality_ids: Option<Vec<i64>>,
    #[serde(rename = "sort_order_id")]
    sort_order_id: Option<i32>,
}

impl DataFileAvro {
    #[allow(dead_code)]
    fn extract_map_fields_from_avro(&mut self, fields: &[(String, AvroValue)]) {
        for (field_name, field_value) in fields {
            match field_name.as_str() {
                "column_sizes" => self.column_sizes = Some(field_value.clone()),
                "value_counts" => self.value_counts = Some(field_value.clone()),
                "null_value_counts" => self.null_value_counts = Some(field_value.clone()),
                "nan_value_counts" => self.nan_value_counts = Some(field_value.clone()),
                "lower_bounds" => self.lower_bounds = Some(field_value.clone()),
                "upper_bounds" => self.upper_bounds = Some(field_value.clone()),
                _ => {}
            }
        }
    }

    fn into_data_file(
        self,
        schema: &super::Schema,
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
            "PARQUET" => super::DataFileFormat::Parquet,
            "AVRO" => super::DataFileFormat::Avro,
            "ORC" => super::DataFileFormat::Orc,
            _ => super::DataFileFormat::Parquet,
        };

        let partition = parse_partition_values(Some(&self.partition));

        let column_sizes = parse_i64_map_from_avro(&self.column_sizes)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let value_counts = parse_i64_map_from_avro(&self.value_counts)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let null_value_counts = parse_i64_map_from_avro(&self.null_value_counts)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();
        let nan_value_counts = parse_i64_map_from_avro(&self.nan_value_counts)
            .into_iter()
            .map(|(k, v)| (k, v as u64))
            .collect();

        let lower_bounds_raw = parse_bytes_map_from_avro(&self.lower_bounds);
        let upper_bounds_raw = parse_bytes_map_from_avro(&self.upper_bounds);
        let lower_bounds = parse_bounds_from_binary(lower_bounds_raw.as_ref(), schema);
        let upper_bounds = parse_bounds_from_binary(upper_bounds_raw.as_ref(), schema);

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
            equality_ids: self
                .equality_ids
                .unwrap_or_default()
                .into_iter()
                .map(|v| v as i32)
                .collect(),
            sort_order_id: self.sort_order_id,
            first_row_id: None,
            partition_spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
}

fn parse_i64_map_from_avro(values: &Option<AvroValue>) -> std::collections::HashMap<i32, i64> {
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

fn parse_bytes_map_from_avro(
    values: &Option<AvroValue>,
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

// NOTE: These helpers mirror provider.rs logic, kept local to avoid cross-module deps.
fn parse_partition_values(json: Option<&serde_json::Value>) -> Vec<Option<Literal>> {
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

fn parse_bounds_from_binary(
    bounds_data: Option<&std::collections::HashMap<i32, Vec<u8>>>,
    schema: &super::Schema,
) -> std::collections::HashMap<i32, Datum> {
    use crate::spec::Type;
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

fn parse_primitive_bound(
    bytes: &[u8],
    prim_type: &crate::spec::PrimitiveType,
) -> Result<Datum, String> {
    use num_bigint::BigInt;
    use num_traits::ToPrimitive;

    use crate::spec::PrimitiveType;
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

/// Metadata about a manifest file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ManifestMetadata {
    /// The schema of the table when the manifest was written.
    pub schema: SchemaRef,
    /// ID of the schema used to write the manifest
    pub schema_id: SchemaId,
    /// The partition spec used to write the manifest.
    pub partition_spec: PartitionSpec,
    /// The format version of the manifest.
    pub format_version: FormatVersion,
    /// Type of content files tracked by the manifest: data or deletes
    pub content: ManifestContentType,
}

impl ManifestMetadata {
    /// Create new manifest metadata.
    pub fn new(
        schema: SchemaRef,
        schema_id: SchemaId,
        partition_spec: PartitionSpec,
        format_version: FormatVersion,
        content: ManifestContentType,
    ) -> Self {
        Self {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        }
    }
}

/// Format version of Iceberg.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FormatVersion {
    /// Version 1
    V1 = 1,
    /// Version 2
    V2 = 2,
}

impl serde::Serialize for FormatVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> serde::Deserialize<'de> for FormatVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        match value {
            1 => Ok(FormatVersion::V1),
            2 => Ok(FormatVersion::V2),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid format version: {}",
                value
            ))),
        }
    }
}

impl Default for FormatVersion {
    fn default() -> Self {
        Self::V2
    }
}

/// Status of a manifest entry.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ManifestStatus {
    /// The data file was added in this snapshot.
    Added,
    /// The data file exists in the table.
    Existing,
    /// The data file was deleted in this snapshot.
    Deleted,
}

/// Content type of a data file.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataContentType {
    /// The file contains data.
    Data,
    /// The file contains position deletes.
    PositionDeletes,
    /// The file contains equality deletes.
    EqualityDeletes,
}

/// File format of a data file.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFileFormat {
    /// Avro format
    Avro,
    /// ORC format
    Orc,
    /// Parquet format
    Parquet,
    /// Puffin format (for delete files)
    Puffin,
}

/// A manifest entry represents a data file in a manifest.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// The status of the data file.
    pub status: ManifestStatus,
    /// The snapshot ID when the data file was added to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// The sequence number when the data file was added to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// The file sequence number indicating when the file was added.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<i64>,
    /// The data file.
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Create a new manifest entry.
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

/// A data file in Iceberg.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// Type of content stored by the data file.
    pub content: DataContentType,
    /// Full URI for the file with FS scheme.
    pub file_path: String,
    /// File format name.
    pub file_format: DataFileFormat,
    /// Partition data tuple.
    pub partition: Vec<Option<Literal>>,
    /// Number of records in this file.
    pub record_count: u64,
    /// Total file size in bytes.
    pub file_size_in_bytes: u64,
    /// Map from column id to the total size on disk of all regions that store the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub column_sizes: HashMap<i32, u64>,
    /// Map from column id to number of values in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub value_counts: HashMap<i32, u64>,
    /// Map from column id to number of null values in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub null_value_counts: HashMap<i32, u64>,
    /// Map from column id to number of NaN values in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub nan_value_counts: HashMap<i32, u64>,
    /// Map from column id to lower bound in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub lower_bounds: HashMap<i32, Datum>,
    /// Map from column id to upper bound in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub upper_bounds: HashMap<i32, Datum>,
    /// Block size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_size_in_bytes: Option<i64>,
    /// Implementation-specific key metadata for encryption.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets for the data file.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub split_offsets: Vec<i64>,
    /// Field ids used to determine row equality in equality delete files.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub equality_ids: Vec<i32>,
    /// ID representing sort order for this file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    /// The _row_id for the first row in the data file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    /// The partition spec id used when writing this data file.
    pub partition_spec_id: i32,
    /// Fully qualified location of a data file that all deletes reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_data_file: Option<String>,
    /// The offset in the file where the content starts (for deletion vectors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_offset: Option<i64>,
    /// The size of the referenced content in bytes (for deletion vectors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_size_in_bytes: Option<i64>,
}

impl DataFile {
    /// Create a new data file builder.
    pub fn builder() -> DataFileBuilder {
        DataFileBuilder::new()
    }

    /// Get the content type of the data file.
    pub fn content_type(&self) -> DataContentType {
        self.content
    }

    /// Get the file path.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Get the file format.
    pub fn file_format(&self) -> DataFileFormat {
        self.file_format
    }

    /// Get the partition values.
    pub fn partition(&self) -> &[Option<Literal>] {
        &self.partition
    }

    /// Get the record count.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Get the file size in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }

    /// Get column sizes.
    pub fn column_sizes(&self) -> &HashMap<i32, u64> {
        &self.column_sizes
    }

    /// Get value counts.
    pub fn value_counts(&self) -> &HashMap<i32, u64> {
        &self.value_counts
    }

    /// Get null value counts.
    pub fn null_value_counts(&self) -> &HashMap<i32, u64> {
        &self.null_value_counts
    }

    /// Get NaN value counts.
    pub fn nan_value_counts(&self) -> &HashMap<i32, u64> {
        &self.nan_value_counts
    }

    /// Get lower bounds.
    pub fn lower_bounds(&self) -> &HashMap<i32, Datum> {
        &self.lower_bounds
    }

    /// Get upper bounds.
    pub fn upper_bounds(&self) -> &HashMap<i32, Datum> {
        &self.upper_bounds
    }
}

/// Builder for creating data files.
#[derive(Debug)]
pub struct DataFileBuilder {
    content: DataContentType,
    file_path: Option<String>,
    file_format: DataFileFormat,
    partition: Vec<Option<Literal>>,
    record_count: u64,
    file_size_in_bytes: u64,
    column_sizes: HashMap<i32, u64>,
    value_counts: HashMap<i32, u64>,
    null_value_counts: HashMap<i32, u64>,
    nan_value_counts: HashMap<i32, u64>,
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    block_size_in_bytes: Option<i64>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Vec<i64>,
    equality_ids: Vec<i32>,
    sort_order_id: Option<i32>,
    first_row_id: Option<i64>,
    partition_spec_id: i32,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl DataFileBuilder {
    /// Create a new data file builder.
    pub fn new() -> Self {
        Self {
            content: DataContentType::Data,
            file_path: None,
            file_format: DataFileFormat::Parquet,
            partition: Vec::new(),
            record_count: 0,
            file_size_in_bytes: 0,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: Vec::new(),
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    /// Set the content type.
    pub fn with_content(mut self, content: DataContentType) -> Self {
        self.content = content;
        self
    }

    /// Set the file path.
    pub fn with_file_path(mut self, file_path: impl ToString) -> Self {
        self.file_path = Some(file_path.to_string());
        self
    }

    /// Set the file format.
    pub fn with_file_format(mut self, file_format: DataFileFormat) -> Self {
        self.file_format = file_format;
        self
    }

    /// Set the partition values.
    pub fn with_partition(mut self, partition: Vec<Option<Literal>>) -> Self {
        self.partition = partition;
        self
    }

    /// Set the record count.
    pub fn with_record_count(mut self, record_count: u64) -> Self {
        self.record_count = record_count;
        self
    }

    /// Set the file size in bytes.
    pub fn with_file_size_in_bytes(mut self, file_size_in_bytes: u64) -> Self {
        self.file_size_in_bytes = file_size_in_bytes;
        self
    }

    /// Set the partition spec id.
    pub fn with_partition_spec_id(mut self, partition_spec_id: i32) -> Self {
        self.partition_spec_id = partition_spec_id;
        self
    }

    /// Add column size.
    pub fn with_column_size(mut self, column_id: i32, size: u64) -> Self {
        self.column_sizes.insert(column_id, size);
        self
    }

    /// Add value count.
    pub fn with_value_count(mut self, column_id: i32, count: u64) -> Self {
        self.value_counts.insert(column_id, count);
        self
    }

    /// Add null value count.
    pub fn with_null_value_count(mut self, column_id: i32, count: u64) -> Self {
        self.null_value_counts.insert(column_id, count);
        self
    }

    /// Add lower bound.
    pub fn with_lower_bound(mut self, column_id: i32, bound: Datum) -> Self {
        self.lower_bounds.insert(column_id, bound);
        self
    }

    /// Add upper bound.
    pub fn with_upper_bound(mut self, column_id: i32, bound: Datum) -> Self {
        self.upper_bounds.insert(column_id, bound);
        self
    }

    /// Set the block size in bytes.
    pub fn with_block_size_in_bytes(mut self, block_size_in_bytes: i64) -> Self {
        self.block_size_in_bytes = Some(block_size_in_bytes);
        self
    }

    /// Set the first row id.
    pub fn with_first_row_id(mut self, first_row_id: i64) -> Self {
        self.first_row_id = Some(first_row_id);
        self
    }

    /// Set the referenced data file path.
    pub fn with_referenced_data_file(mut self, path: impl ToString) -> Self {
        self.referenced_data_file = Some(path.to_string());
        self
    }

    /// Set the content offset and size in bytes.
    pub fn with_content_offset_and_size(mut self, offset: i64, size_in_bytes: i64) -> Self {
        self.content_offset = Some(offset);
        self.content_size_in_bytes = Some(size_in_bytes);
        self
    }

    /// Build the data file.
    pub fn build(self) -> Result<DataFile, String> {
        let file_path = self.file_path.ok_or("file_path is required")?;

        Ok(DataFile {
            content: self.content,
            file_path,
            file_format: self.file_format,
            partition: self.partition,
            record_count: self.record_count,
            file_size_in_bytes: self.file_size_in_bytes,
            column_sizes: self.column_sizes,
            value_counts: self.value_counts,
            null_value_counts: self.null_value_counts,
            nan_value_counts: self.nan_value_counts,
            lower_bounds: self.lower_bounds,
            upper_bounds: self.upper_bounds,
            block_size_in_bytes: self.block_size_in_bytes,
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets,
            equality_ids: self.equality_ids,
            sort_order_id: self.sort_order_id,
            first_row_id: self.first_row_id,
            partition_spec_id: self.partition_spec_id,
            referenced_data_file: self.referenced_data_file,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
        })
    }
}

impl Default for DataFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}
