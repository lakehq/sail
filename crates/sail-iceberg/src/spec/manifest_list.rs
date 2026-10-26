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

// [CREDIT]: https://raw.githubusercontent.com/apache/iceberg-rust/dc349284a4204c1a56af47fb3177ace6f9e899a0/crates/iceberg/src/spec/manifest_list.rs

use apache_avro::{Reader as AvroReader, Schema as AvroSchema, from_value as avro_from_value};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::spec::FormatVersion;
use crate::spec::avro_utils::create_avro_writer;
mod schema;

pub const UNASSIGNED_SEQUENCE_NUMBER: i64 = -1;

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestList {
    /// Entries in a manifest list.
    pub entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Create a new manifest list.
    pub fn new(entries: Vec<ManifestFile>) -> Self {
        Self { entries }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn into_entries(self) -> Vec<ManifestFile> {
        self.entries
    }

    /// Parse using the file's writer schema, independently of the current table version.
    /// Historical lists and legacy Sail lists may have a different format from the table.
    pub fn parse_with_version(bs: &[u8], _version: FormatVersion) -> Result<ManifestList, String> {
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
        let AvroSchema::Record(record) = reader.writer_schema() else {
            return Err("Manifest list writer schema must be a record".into());
        };
        let has_field = |name: &str| record.fields.iter().any(|field| field.name == name);
        let version = match (
            has_field("content"),
            has_field("sequence_number"),
            has_field("min_sequence_number"),
            has_field("first_row_id"),
        ) {
            (_, false, false, false) => FormatVersion::V1,
            (true, true, true, false) => FormatVersion::V2,
            (true, true, true, true) => FormatVersion::V3,
            _ => return Err("Incomplete V2 manifest list tracking fields".into()),
        };
        let entries = reader
            .map(|value| {
                let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
                match version {
                    FormatVersion::V1 => {
                        let file = avro_from_value::<_serde::ManifestFileV1>(&value)
                            .map_err(|e| format!("Avro V1 manifest list decode error: {e}"))?;
                        ManifestFile::try_from(file)
                    }
                    FormatVersion::V2 => {
                        let file = avro_from_value::<_serde::ManifestFileV2>(&value)
                            .map_err(|e| format!("Avro V2 manifest list decode error: {e}"))?;
                        ManifestFile::try_from(file)
                    }
                    FormatVersion::V3 => {
                        let file = avro_from_value::<_serde::ManifestFileV3>(&value)
                            .map_err(|e| format!("Avro V3 manifest list decode error: {e}"))?;
                        ManifestFile::try_from(file)
                    }
                }
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ManifestList::new(entries))
    }
}

/// Snapshot-level metadata for a manifest list write; not inferred from its entries.
#[derive(Debug, Clone, Copy)]
pub struct ManifestListWriteContext {
    pub version: FormatVersion,
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub first_row_id: Option<i64>,
}

pub struct ManifestListWriter {
    entries: Vec<ManifestFile>,
}

impl Default for ManifestListWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestListWriter {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn append(&mut self, manifest: ManifestFile) {
        self.entries.push(manifest);
    }

    pub fn finish(self) -> ManifestList {
        ManifestList::new(self.entries)
    }

    #[cfg(test)]
    pub(crate) fn to_bytes(&self, version: FormatVersion) -> Result<Vec<u8>, String> {
        self.to_bytes_with_context(ManifestListWriteContext {
            version,
            snapshot_id: 7,
            parent_snapshot_id: None,
            sequence_number: 0,
            first_row_id: (version == FormatVersion::V3).then_some(0),
        })
    }

    pub fn to_bytes_with_context(
        &self,
        context: ManifestListWriteContext,
    ) -> Result<Vec<u8>, String> {
        let version = context.version;
        use crate::spec::manifest_list::schema::{
            MANIFEST_LIST_AVRO_SCHEMA_V1, MANIFEST_LIST_AVRO_SCHEMA_V2,
            MANIFEST_LIST_AVRO_SCHEMA_V3,
        };

        let schema = match version {
            FormatVersion::V1 => &MANIFEST_LIST_AVRO_SCHEMA_V1,
            FormatVersion::V2 => &MANIFEST_LIST_AVRO_SCHEMA_V2,
            FormatVersion::V3 => &MANIFEST_LIST_AVRO_SCHEMA_V3,
        };
        let mut writer = create_avro_writer(schema, Vec::new())
            .map_err(|error| format!("Avro writer creation error: {error}"))?;
        let mut metadata = vec![
            ("format-version", (version as u8).to_string()),
            ("snapshot-id", context.snapshot_id.to_string()),
            (
                "parent-snapshot-id",
                context
                    .parent_snapshot_id
                    .map_or_else(|| "null".into(), |id| id.to_string()),
            ),
        ];
        if version >= FormatVersion::V2 {
            metadata.push(("sequence-number", context.sequence_number.to_string()));
        }
        if version >= FormatVersion::V3 {
            metadata.push((
                "first-row-id",
                context
                    .first_row_id
                    .map_or_else(|| "null".into(), |id| id.to_string()),
            ));
        }
        for (name, value) in metadata {
            writer
                .add_user_metadata(name.into(), value)
                .map_err(|error| format!("Avro metadata error: {error}"))?;
        }

        for mf in &self.entries {
            if version >= FormatVersion::V2
                && (mf.added_files_count.is_none()
                    || mf.existing_files_count.is_none()
                    || mf.deleted_files_count.is_none()
                    || mf.added_rows_count.is_none()
                    || mf.existing_rows_count.is_none()
                    || mf.deleted_rows_count.is_none())
            {
                return Err("Missing required V2 counts/rows fields".to_string());
            }
            let content = match mf.content {
                ManifestContentType::Data => 0,
                ManifestContentType::Deletes => 1,
            };
            let partitions = mf.partitions.clone().map(|ps| {
                ps.into_iter()
                    .map(|p| FieldSummaryAvro {
                        contains_null: p.contains_null,
                        contains_nan: p.contains_nan,
                        lower_bound: p.lower_bound_bytes.map(ByteBuf::from),
                        upper_bound: p.upper_bound_bytes.map(ByteBuf::from),
                    })
                    .collect()
            });
            match version {
                FormatVersion::V1 => {
                    let v1 = _serde::ManifestFileV1 {
                        content,
                        manifest_path: mf.manifest_path.clone(),
                        manifest_length: mf.manifest_length,
                        partition_spec_id: mf.partition_spec_id,
                        added_snapshot_id: mf.added_snapshot_id,
                        added_files_count: mf.added_files_count,
                        existing_files_count: mf.existing_files_count,
                        deleted_files_count: mf.deleted_files_count,
                        added_rows_count: mf.added_rows_count,
                        existing_rows_count: mf.existing_rows_count,
                        deleted_rows_count: mf.deleted_rows_count,
                        partitions,
                        key_metadata: mf.key_metadata.clone().map(ByteBuf::from),
                    };
                    writer
                        .append_ser(v1)
                        .map_err(|e| format!("Avro append error: {e}"))?;
                }
                FormatVersion::V3 => {
                    let v3 = _serde::ManifestFileV3 {
                        manifest_path: mf.manifest_path.clone(),
                        manifest_length: mf.manifest_length,
                        partition_spec_id: mf.partition_spec_id,
                        content,
                        sequence_number: mf.sequence_number,
                        min_sequence_number: mf.min_sequence_number,
                        added_snapshot_id: mf.added_snapshot_id,
                        added_files_count: mf.added_files_count.unwrap_or(0),
                        existing_files_count: mf.existing_files_count.unwrap_or(0),
                        deleted_files_count: mf.deleted_files_count.unwrap_or(0),
                        added_rows_count: mf.added_rows_count.unwrap_or(0),
                        existing_rows_count: mf.existing_rows_count.unwrap_or(0),
                        deleted_rows_count: mf.deleted_rows_count.unwrap_or(0),
                        partitions,
                        key_metadata: mf.key_metadata.clone().map(ByteBuf::from),
                        first_row_id: mf.first_row_id,
                    };
                    writer
                        .append_ser(v3)
                        .map_err(|e| format!("Avro append error: {e}"))?;
                }
                FormatVersion::V2 => {
                    let v2 = _serde::ManifestFileV2 {
                        manifest_path: mf.manifest_path.clone(),
                        manifest_length: mf.manifest_length,
                        partition_spec_id: mf.partition_spec_id,
                        content,
                        sequence_number: mf.sequence_number,
                        min_sequence_number: mf.min_sequence_number,
                        added_snapshot_id: mf.added_snapshot_id,
                        added_files_count: mf.added_files_count.unwrap_or(0),
                        existing_files_count: mf.existing_files_count.unwrap_or(0),
                        deleted_files_count: mf.deleted_files_count.unwrap_or(0),
                        added_rows_count: mf.added_rows_count.unwrap_or(0),
                        existing_rows_count: mf.existing_rows_count.unwrap_or(0),
                        deleted_rows_count: mf.deleted_rows_count.unwrap_or(0),
                        partitions,
                        key_metadata: mf.key_metadata.clone().map(ByteBuf::from),
                    };
                    writer
                        .append_ser(v2)
                        .map_err(|e| format!("Avro append error: {e}"))?;
                }
            }
        }

        writer
            .into_inner()
            .map_err(|e| format!("Avro writer finalize error: {e}"))
    }
}

// apache-avro's Option deserializer requires a union, but older writers may use
// a bare value for an optional field. deserialize_any handles both encodings.
fn deserialize_optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Optional<T> {
        Value(T),
        Null(()),
    }
    Optional::<T>::deserialize(deserializer).map(|value| match value {
        Optional::Value(value) => Some(value),
        Optional::Null(()) => None,
    })
}

fn deserialize_optional_bytes<'de, D>(deserializer: D) -> Result<Option<ByteBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct OptionalBytesVisitor;
    impl<'de> serde::de::Visitor<'de> for OptionalBytesVisitor {
        type Value = Option<ByteBuf>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bytes or null")
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
            Ok(Some(ByteBuf::from(value)))
        }
    }
    deserializer.deserialize_any(OptionalBytesVisitor)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldSummaryAvro {
    contains_null: bool,
    #[serde(default, deserialize_with = "deserialize_optional")]
    contains_nan: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_bytes")]
    lower_bound: Option<ByteBuf>,
    #[serde(default, deserialize_with = "deserialize_optional_bytes")]
    upper_bound: Option<ByteBuf>,
}

impl From<FieldSummaryAvro> for FieldSummary {
    fn from(summary: FieldSummaryAvro) -> Self {
        let mut field_summary = FieldSummary::new(summary.contains_null);
        if let Some(contains_nan) = summary.contains_nan {
            field_summary = field_summary.with_contains_nan(contains_nan);
        }
        field_summary.lower_bound_bytes = summary.lower_bound.map(ByteBuf::into_vec);
        field_summary.upper_bound_bytes = summary.upper_bound.map(ByteBuf::into_vec);
        field_summary
    }
}

impl TryFrom<_serde::ManifestFileV2> for ManifestFile {
    type Error = String;

    fn try_from(avro: _serde::ManifestFileV2) -> Result<Self, Self::Error> {
        let content = ManifestContentType::try_from(avro.content)?;

        let partitions = avro
            .partitions
            .map(|summaries| summaries.into_iter().map(FieldSummary::from).collect());

        Ok(ManifestFile {
            manifest_path: avro.manifest_path,
            manifest_length: avro.manifest_length,
            partition_spec_id: avro.partition_spec_id,
            content,
            sequence_number: avro.sequence_number,
            min_sequence_number: avro.min_sequence_number,
            added_snapshot_id: avro.added_snapshot_id,
            added_files_count: Some(avro.added_files_count),
            existing_files_count: Some(avro.existing_files_count),
            deleted_files_count: Some(avro.deleted_files_count),
            added_rows_count: Some(avro.added_rows_count),
            existing_rows_count: Some(avro.existing_rows_count),
            deleted_rows_count: Some(avro.deleted_rows_count),
            partitions,
            key_metadata: avro.key_metadata.map(ByteBuf::into_vec),
            first_row_id: None,
        })
    }
}

impl TryFrom<_serde::ManifestFileV3> for ManifestFile {
    type Error = String;

    fn try_from(avro: _serde::ManifestFileV3) -> Result<Self, Self::Error> {
        let content = ManifestContentType::try_from(avro.content)?;

        let partitions = avro
            .partitions
            .map(|summaries| summaries.into_iter().map(FieldSummary::from).collect());

        Ok(ManifestFile {
            manifest_path: avro.manifest_path,
            manifest_length: avro.manifest_length,
            partition_spec_id: avro.partition_spec_id,
            content,
            sequence_number: avro.sequence_number,
            min_sequence_number: avro.min_sequence_number,
            added_snapshot_id: avro.added_snapshot_id,
            added_files_count: Some(avro.added_files_count),
            existing_files_count: Some(avro.existing_files_count),
            deleted_files_count: Some(avro.deleted_files_count),
            added_rows_count: Some(avro.added_rows_count),
            existing_rows_count: Some(avro.existing_rows_count),
            deleted_rows_count: Some(avro.deleted_rows_count),
            partitions,
            key_metadata: avro.key_metadata.map(ByteBuf::into_vec),
            first_row_id: avro.first_row_id,
        })
    }
}

/// Status of a manifest file in a manifest list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestFileStatus {
    /// The manifest file was added in this snapshot.
    Added,
    /// The manifest file was inherited from the parent snapshot.
    Existing,
    /// The manifest file was deleted in this snapshot.
    Deleted,
}

/// Content type of a manifest file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestContentType {
    /// The manifest contains data files.
    Data,
    /// The manifest contains delete files.
    Deletes,
}

impl TryFrom<i32> for ManifestContentType {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Deletes),
            _ => Err(format!("Invalid Iceberg manifest content: {value}")),
        }
    }
}

/// A manifest file in a manifest list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ManifestFile {
    /// The path to the manifest file.
    pub manifest_path: String,
    /// The length of the manifest file in bytes.
    pub manifest_length: i64,
    /// The ID of the partition spec used to write the manifest.
    pub partition_spec_id: i32,
    /// The content type of the manifest file.
    pub content: ManifestContentType,
    /// The sequence number when the manifest was added to the table.
    pub sequence_number: i64,
    /// The minimum sequence number of all data files in the manifest.
    pub min_sequence_number: i64,
    /// The snapshot ID when the manifest was added to the table.
    pub added_snapshot_id: i64,
    /// The number of files added in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_files_count: Option<i32>,
    /// The number of existing files in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_files_count: Option<i32>,
    /// The number of deleted files in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_files_count: Option<i32>,
    /// The number of rows added in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_rows_count: Option<i64>,
    /// The number of existing rows in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_rows_count: Option<i64>,
    /// The number of deleted rows in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_rows_count: Option<i64>,
    /// A list of field summaries for each partition field in the spec.
    /// Each field in the list corresponds to a field in the manifest file's partition spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<FieldSummary>>,
    /// Implementation-specific key metadata for encryption.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    /// The starting row ID to assign to rows in this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
}

impl ManifestFile {
    /// Create a new manifest file builder.
    pub fn builder() -> ManifestFileBuilder {
        ManifestFileBuilder::new()
    }

    /// Get the total number of files in this manifest.
    pub fn total_files_count(&self) -> i32 {
        self.added_files_count.unwrap_or(0)
            + self.existing_files_count.unwrap_or(0)
            + self.deleted_files_count.unwrap_or(0)
    }

    /// Get the total number of rows in this manifest.
    pub fn total_rows_count(&self) -> i64 {
        self.added_rows_count.unwrap_or(0)
            + self.existing_rows_count.unwrap_or(0)
            + self.deleted_rows_count.unwrap_or(0)
    }

    /// Whether the manifest contains any added files
    pub fn has_added_files(&self) -> bool {
        self.added_files_count.unwrap_or(0) > 0
    }

    /// Whether the manifest contains any deleted files
    pub fn has_deleted_files(&self) -> bool {
        self.deleted_files_count.unwrap_or(0) > 0
    }
}

/// Field summary for partition fields in a manifest file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FieldSummary {
    /// Whether the partition field contains null values.
    pub contains_null: bool,
    /// Whether the partition field contains NaN values (only for float and double).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains_nan: Option<bool>,
    /// The minimum value of the partition field (binary encoded per spec).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound_bytes: Option<Vec<u8>>,
    /// The maximum value of the partition field (binary encoded per spec).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound_bytes: Option<Vec<u8>>,
}

impl FieldSummary {
    /// Create a new field summary.
    pub fn new(contains_null: bool) -> Self {
        Self {
            contains_null,
            contains_nan: None,
            lower_bound_bytes: None,
            upper_bound_bytes: None,
        }
    }

    /// Set whether the field contains NaN values.
    pub fn with_contains_nan(mut self, contains_nan: bool) -> Self {
        self.contains_nan = Some(contains_nan);
        self
    }

    /// Set the lower bound of the field.
    pub fn with_lower_bound_bytes(mut self, lower: Vec<u8>) -> Self {
        self.lower_bound_bytes = Some(lower);
        self
    }

    /// Set the upper bound of the field.
    pub fn with_upper_bound_bytes(mut self, upper: Vec<u8>) -> Self {
        self.upper_bound_bytes = Some(upper);
        self
    }
}

/// Builder for creating manifest files.
#[derive(Debug)]
pub struct ManifestFileBuilder {
    manifest_path: Option<String>,
    manifest_length: i64,
    partition_spec_id: i32,
    content: ManifestContentType,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
    added_files_count: Option<i32>,
    existing_files_count: Option<i32>,
    deleted_files_count: Option<i32>,
    added_rows_count: Option<i64>,
    existing_rows_count: Option<i64>,
    deleted_rows_count: Option<i64>,
    partitions: Option<Vec<FieldSummary>>,
    key_metadata: Option<Vec<u8>>,
    first_row_id: Option<i64>,
}

impl ManifestFileBuilder {
    /// Create a new manifest file builder.
    pub fn new() -> Self {
        Self {
            manifest_path: None,
            manifest_length: 0,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
            added_snapshot_id: 0,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    /// Set the manifest path.
    pub fn with_manifest_path(mut self, manifest_path: impl ToString) -> Self {
        self.manifest_path = Some(manifest_path.to_string());
        self
    }

    /// Set the manifest length.
    pub fn with_manifest_length(mut self, manifest_length: i64) -> Self {
        self.manifest_length = manifest_length;
        self
    }

    /// Set the partition spec id.
    pub fn with_partition_spec_id(mut self, partition_spec_id: i32) -> Self {
        self.partition_spec_id = partition_spec_id;
        self
    }

    /// Set the content type.
    pub fn with_content(mut self, content: ManifestContentType) -> Self {
        self.content = content;
        self
    }

    /// Set the sequence number.
    pub fn with_sequence_number(mut self, sequence_number: i64) -> Self {
        self.sequence_number = sequence_number;
        self
    }

    /// Set the minimum sequence number.
    pub fn with_min_sequence_number(mut self, min_sequence_number: i64) -> Self {
        self.min_sequence_number = min_sequence_number;
        self
    }

    /// Set the added snapshot id.
    pub fn with_added_snapshot_id(mut self, added_snapshot_id: i64) -> Self {
        self.added_snapshot_id = added_snapshot_id;
        self
    }

    /// Set the file counts.
    pub fn with_file_counts(mut self, added: i32, existing: i32, deleted: i32) -> Self {
        self.added_files_count = Some(added);
        self.existing_files_count = Some(existing);
        self.deleted_files_count = Some(deleted);
        self
    }

    /// Set the row counts.
    pub fn with_row_counts(mut self, added: i64, existing: i64, deleted: i64) -> Self {
        self.added_rows_count = Some(added);
        self.existing_rows_count = Some(existing);
        self.deleted_rows_count = Some(deleted);
        self
    }

    /// Set the partitions.
    pub fn with_partitions(mut self, partitions: Vec<FieldSummary>) -> Self {
        self.partitions = Some(partitions);
        self
    }

    /// Set the key metadata.
    pub fn with_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set the first row ID.
    pub fn with_first_row_id(mut self, first_row_id: i64) -> Self {
        self.first_row_id = Some(first_row_id);
        self
    }

    /// Build the manifest file.
    pub fn build(self) -> Result<ManifestFile, String> {
        let manifest_path = self.manifest_path.ok_or("manifest_path is required")?;

        Ok(ManifestFile {
            manifest_path,
            manifest_length: self.manifest_length,
            partition_spec_id: self.partition_spec_id,
            content: self.content,
            sequence_number: self.sequence_number,
            min_sequence_number: self.min_sequence_number,
            added_snapshot_id: self.added_snapshot_id,
            added_files_count: self.added_files_count,
            existing_files_count: self.existing_files_count,
            deleted_files_count: self.deleted_files_count,
            added_rows_count: self.added_rows_count,
            existing_rows_count: self.existing_rows_count,
            deleted_rows_count: self.deleted_rows_count,
            partitions: self.partitions,
            key_metadata: self.key_metadata,
            first_row_id: self.first_row_id,
        })
    }
}

impl Default for ManifestFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) mod _serde {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ManifestFileV1 {
        #[serde(default, skip_serializing)]
        pub content: i32,
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub added_snapshot_id: i64,
        #[serde(
            default,
            alias = "added_data_files_count",
            deserialize_with = "deserialize_optional"
        )]
        pub added_files_count: Option<i32>,
        #[serde(
            default,
            alias = "existing_data_files_count",
            deserialize_with = "deserialize_optional"
        )]
        pub existing_files_count: Option<i32>,
        #[serde(
            default,
            alias = "deleted_data_files_count",
            deserialize_with = "deserialize_optional"
        )]
        pub deleted_files_count: Option<i32>,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub added_rows_count: Option<i64>,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub existing_rows_count: Option<i64>,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub deleted_rows_count: Option<i64>,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub partitions: Option<Vec<FieldSummaryAvro>>,
        #[serde(default, deserialize_with = "deserialize_optional_bytes")]
        pub key_metadata: Option<ByteBuf>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ManifestFileV2 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub content: i32,
        pub sequence_number: i64,
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        #[serde(alias = "added_data_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count")]
        pub deleted_files_count: i32,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub partitions: Option<Vec<FieldSummaryAvro>>,
        #[serde(default, deserialize_with = "deserialize_optional_bytes")]
        pub key_metadata: Option<ByteBuf>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ManifestFileV3 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub content: i32,
        pub sequence_number: i64,
        pub min_sequence_number: i64,
        pub added_snapshot_id: i64,
        #[serde(alias = "added_data_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count")]
        pub deleted_files_count: i32,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub partitions: Option<Vec<FieldSummaryAvro>>,
        #[serde(default, deserialize_with = "deserialize_optional_bytes")]
        pub key_metadata: Option<ByteBuf>,
        #[serde(default, deserialize_with = "deserialize_optional")]
        pub first_row_id: Option<i64>,
    }
}

impl TryFrom<_serde::ManifestFileV1> for ManifestFile {
    type Error = String;

    fn try_from(v1: _serde::ManifestFileV1) -> Result<Self, Self::Error> {
        if v1.content != 0 {
            return Err(format!("Invalid V1 manifest content: {}", v1.content));
        }
        Ok(ManifestFile {
            manifest_path: v1.manifest_path,
            manifest_length: v1.manifest_length,
            partition_spec_id: v1.partition_spec_id,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: v1.added_snapshot_id,
            added_files_count: v1.added_files_count,
            existing_files_count: v1.existing_files_count,
            deleted_files_count: v1.deleted_files_count,
            added_rows_count: v1.added_rows_count,
            existing_rows_count: v1.existing_rows_count,
            deleted_rows_count: v1.deleted_rows_count,
            partitions: v1
                .partitions
                .map(|v| v.into_iter().map(FieldSummary::from).collect()),
            key_metadata: v1.key_metadata.map(ByteBuf::into_vec),
            first_row_id: None,
        })
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use apache_avro::types::Value as AvroValue;
    use serde_json::{Value as JsonValue, json};

    use super::*;

    const VERSIONS: [FormatVersion; 3] = [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3];

    fn manifest_file() -> ManifestFile {
        ManifestFile {
            manifest_path: "metadata/manifest.avro".to_string(),
            manifest_length: 10,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 3,
            min_sequence_number: 3,
            added_snapshot_id: 7,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(2),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    // Independent wire fixtures: not generated by ManifestListWriter or its schemas.
    fn record(
        name: &str,
        fields: Vec<(&str, JsonValue, AvroValue, bool)>,
    ) -> (JsonValue, AvroValue) {
        let (schemas, values): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .map(|(name, ty, value, optional)| {
                let (ty, value) = if optional {
                    (json!(["null", ty]), AvroValue::Union(1, Box::new(value)))
                } else {
                    (ty, value)
                };
                (json!({"name": name, "type": ty}), (name.to_string(), value))
            })
            .unzip();
        (
            json!({"type": "record", "name": name, "fields": schemas}),
            AvroValue::Record(values),
        )
    }

    fn fixture(version: FormatVersion, unions: bool) -> (JsonValue, AvroValue, ManifestFile) {
        let (summary_schema, summary) = record(
            "field_summary",
            vec![
                (
                    "contains_null",
                    json!("boolean"),
                    AvroValue::Boolean(true),
                    false,
                ),
                (
                    "contains_nan",
                    json!("boolean"),
                    AvroValue::Boolean(false),
                    unions,
                ),
                (
                    "lower_bound",
                    json!("bytes"),
                    AvroValue::Bytes(vec![0, 255, 1]),
                    unions,
                ),
                (
                    "upper_bound",
                    json!("bytes"),
                    AvroValue::Bytes(vec![128, 0, 2]),
                    unions,
                ),
            ],
        );
        let mut fields = vec![
            (
                "manifest_path",
                json!("string"),
                AvroValue::String("metadata/manifest.avro".into()),
                false,
            ),
            ("manifest_length", json!("long"), AvroValue::Long(10), false),
            ("partition_spec_id", json!("int"), AvroValue::Int(0), false),
            (
                "added_snapshot_id",
                json!("long"),
                AvroValue::Long(7),
                false,
            ),
        ];
        if version != FormatVersion::V1 {
            fields.extend([
                ("content", json!("int"), AvroValue::Int(1), false),
                ("sequence_number", json!("long"), AvroValue::Long(3), false),
                (
                    "min_sequence_number",
                    json!("long"),
                    AvroValue::Long(2),
                    false,
                ),
            ]);
        }
        let optional_counts = version == FormatVersion::V1 && unions;
        fields.extend([
            (
                "added_data_files_count",
                json!("int"),
                AvroValue::Int(1),
                optional_counts,
            ),
            (
                "existing_data_files_count",
                json!("int"),
                AvroValue::Int(0),
                optional_counts,
            ),
            (
                "deleted_data_files_count",
                json!("int"),
                AvroValue::Int(0),
                optional_counts,
            ),
            (
                "added_rows_count",
                json!("long"),
                AvroValue::Long(2),
                optional_counts,
            ),
            (
                "existing_rows_count",
                json!("long"),
                AvroValue::Long(0),
                optional_counts,
            ),
            (
                "deleted_rows_count",
                json!("long"),
                AvroValue::Long(0),
                optional_counts,
            ),
            (
                "partitions",
                json!({"type": "array", "items": summary_schema}),
                AvroValue::Array(vec![summary]),
                unions,
            ),
            (
                "key_metadata",
                json!("bytes"),
                AvroValue::Bytes(vec![255, 0, 128]),
                unions,
            ),
        ]);
        if version == FormatVersion::V3 {
            fields.push(("first_row_id", json!("long"), AvroValue::Long(1234), unions));
        }
        let mut expected = manifest_file();
        expected.content = if version == FormatVersion::V1 {
            ManifestContentType::Data
        } else {
            ManifestContentType::Deletes
        };
        expected.sequence_number = if version == FormatVersion::V1 { 0 } else { 3 };
        expected.min_sequence_number = if version == FormatVersion::V1 { 0 } else { 2 };
        expected.partitions = Some(vec![
            FieldSummary::new(true)
                .with_contains_nan(false)
                .with_lower_bound_bytes(vec![0, 255, 1])
                .with_upper_bound_bytes(vec![128, 0, 2]),
        ]);
        expected.key_metadata = Some(vec![255, 0, 128]);
        expected.first_row_id = (version == FormatVersion::V3).then_some(1234);
        let (schema, value) = record("manifest_file", fields);
        (schema, value, expected)
    }

    fn encode(schema: &JsonValue, values: &[AvroValue]) -> Vec<u8> {
        let schema = AvroSchema::parse_str(&schema.to_string()).expect("fixture schema");
        let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("fixture writer");
        for value in values {
            writer.append_value(value.clone()).expect("fixture record");
        }
        writer.into_inner().expect("fixture bytes")
    }

    fn replace_field(
        schema: &mut JsonValue,
        value: &mut AvroValue,
        name: &str,
        replacement: Option<(JsonValue, AvroValue)>,
    ) {
        let fields = schema["fields"].as_array_mut().expect("schema fields");
        let AvroValue::Record(values) = value else {
            unreachable!("record fixture")
        };
        fields.retain(|field| field["name"] != name);
        values.retain(|(field, _)| field != name);
        if let Some((ty, value)) = replacement {
            fields.push(json!({"name": name, "type": ty}));
            values.push((name.into(), value));
        }
    }

    fn assert_rejected(schema: &JsonValue, value: AvroValue) {
        let bytes = encode(schema, &[value]);
        for hint in VERSIONS {
            assert!(
                ManifestList::parse_with_version(&bytes, hint).is_err(),
                "hint {hint:?}"
            );
        }
    }

    #[test]
    fn reader_uses_writer_schema_for_all_version_hints() {
        for version in VERSIONS {
            for unions in [false, true] {
                let (schema, value, expected) = fixture(version, unions);
                let bytes = encode(&schema, &[value]);
                for hint in VERSIONS {
                    let parsed =
                        ManifestList::parse_with_version(&bytes, hint).expect("typed decode");
                    assert_eq!(
                        parsed.entries,
                        vec![expected.clone()],
                        "writer {version:?}, hint {hint:?}, unions {unions}"
                    );
                }
            }
        }
    }

    #[test]
    fn writer_uses_r508_and_preserves_optional_values() {
        for version in VERSIONS {
            let (_, _, mut expected) = fixture(version, true);
            expected.content = ManifestContentType::Data;
            expected.partitions.as_mut().expect("summaries").extend([
                FieldSummary::new(false).with_contains_nan(true),
                FieldSummary::new(true),
            ]);
            let mut writer = ManifestListWriter::new();
            writer.append(expected.clone());
            let bytes = writer.to_bytes(version).expect("write manifest list");
            let reader = AvroReader::new(bytes.as_slice()).expect("reader");
            let schema = serde_json::to_value(reader.writer_schema()).expect("schema JSON");
            let partitions = schema["fields"]
                .as_array()
                .expect("fields")
                .iter()
                .find(|field| field["name"] == "partitions")
                .expect("partitions");
            assert_eq!(partitions["type"][1]["items"]["name"], "r508");
            if version == FormatVersion::V1 {
                assert!(
                    schema["fields"]
                        .as_array()
                        .expect("fields")
                        .iter()
                        .all(|field| field["name"] != "content")
                );
            }
            for hint in VERSIONS {
                assert_eq!(
                    ManifestList::parse_with_version(&bytes, hint)
                        .expect("read")
                        .entries,
                    vec![expected.clone()]
                );
            }
        }
    }

    #[test]
    fn v1_reader_accepts_explicit_data_content() {
        for unions in [false, true] {
            let (mut schema, mut value, expected) = fixture(FormatVersion::V1, unions);
            replace_field(
                &mut schema,
                &mut value,
                "content",
                Some((json!("int"), AvroValue::Int(0))),
            );
            let bytes = encode(&schema, &[value]);
            for hint in VERSIONS {
                assert_eq!(
                    ManifestList::parse_with_version(&bytes, hint)
                        .expect("V1 explicit data content")
                        .entries,
                    vec![expected.clone()],
                    "hint {hint:?}, unions {unions}"
                );
            }
        }
    }

    #[test]
    fn v1_reader_rejects_invalid_explicit_content() {
        for replacement in [
            (json!("int"), AvroValue::Int(1)),
            (json!("int"), AvroValue::Int(-1)),
            (json!("int"), AvroValue::Int(2)),
            (json!("null"), AvroValue::Null),
            (
                json!(["null", "int"]),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
            ),
            (json!("string"), AvroValue::String("0".into())),
            (json!("boolean"), AvroValue::Boolean(false)),
        ] {
            let (mut schema, mut value, _) = fixture(FormatVersion::V1, false);
            replace_field(&mut schema, &mut value, "content", Some(replacement));
            assert_rejected(&schema, value);
        }
    }

    #[test]
    fn v1_reader_preserves_missing_and_null_counts() {
        for missing in [false, true] {
            let (mut schema, mut value, mut expected) = fixture(FormatVersion::V1, true);
            for name in [
                "added_data_files_count",
                "existing_data_files_count",
                "deleted_data_files_count",
                "added_rows_count",
                "existing_rows_count",
                "deleted_rows_count",
            ] {
                let ty = if name.ends_with("files_count") {
                    "int"
                } else {
                    "long"
                };
                let replacement = (!missing).then(|| {
                    (
                        json!(["null", ty]),
                        AvroValue::Union(0, Box::new(AvroValue::Null)),
                    )
                });
                replace_field(&mut schema, &mut value, name, replacement);
            }
            expected.added_files_count = None;
            expected.existing_files_count = None;
            expected.deleted_files_count = None;
            expected.added_rows_count = None;
            expected.existing_rows_count = None;
            expected.deleted_rows_count = None;
            let bytes = encode(&schema, &[value]);
            for hint in VERSIONS {
                assert_eq!(
                    ManifestList::parse_with_version(&bytes, hint)
                        .expect("V1 unknown counts")
                        .entries,
                    vec![expected.clone()]
                );
            }
        }
    }

    #[test]
    fn reader_preserves_absent_or_null_optional_fields() {
        for version in VERSIONS {
            for missing in [false, true] {
                let (mut schema, mut value, mut expected) = fixture(version, true);
                for name in ["partitions", "key_metadata", "first_row_id"] {
                    if name == "first_row_id" && version != FormatVersion::V3 {
                        continue;
                    }
                    let replacement = (!missing).then(|| (json!("null"), AvroValue::Null));
                    replace_field(&mut schema, &mut value, name, replacement);
                }
                expected.partitions = None;
                expected.key_metadata = None;
                expected.first_row_id = None;
                let bytes = encode(&schema, &[value]);
                for hint in VERSIONS {
                    assert_eq!(
                        ManifestList::parse_with_version(&bytes, hint)
                            .expect("optional fields")
                            .entries,
                        vec![expected.clone()]
                    );
                }
            }
        }
    }

    #[test]
    fn reader_rejects_incomplete_tracking_shapes() {
        for name in ["content", "sequence_number", "min_sequence_number"] {
            let (mut schema, mut value, _) = fixture(FormatVersion::V2, false);
            replace_field(&mut schema, &mut value, name, None);
            assert_rejected(&schema, value);
        }
        let (mut schema, mut value, _) = fixture(FormatVersion::V1, false);
        replace_field(
            &mut schema,
            &mut value,
            "first_row_id",
            Some((json!("long"), AvroValue::Long(1))),
        );
        assert_rejected(&schema, value);
    }

    #[test]
    fn reader_rejects_unknown_content_and_malformed_fields() {
        for version in [FormatVersion::V2, FormatVersion::V3] {
            for content in [-1, 2, i32::MAX] {
                let (mut schema, mut value, _) = fixture(version, false);
                replace_field(
                    &mut schema,
                    &mut value,
                    "content",
                    Some((json!("int"), AvroValue::Int(content))),
                );
                assert_rejected(&schema, value);
            }
        }
        for name in [
            "content",
            "sequence_number",
            "min_sequence_number",
            "added_data_files_count",
            "added_rows_count",
            "key_metadata",
            "first_row_id",
        ] {
            let (mut schema, mut value, _) = fixture(FormatVersion::V3, false);
            replace_field(
                &mut schema,
                &mut value,
                name,
                Some((json!("string"), AvroValue::String("invalid".into()))),
            );
            assert_rejected(&schema, value);
        }
        for replacement in [None, Some((json!("null"), AvroValue::Null))] {
            let (mut schema, mut value, _) = fixture(FormatVersion::V2, false);
            replace_field(
                &mut schema,
                &mut value,
                "added_data_files_count",
                replacement,
            );
            assert_rejected(&schema, value);
        }
    }

    #[test]
    fn reader_rejects_malformed_partition_summaries() {
        for version in VERSIONS {
            for name in [
                "contains_null",
                "contains_nan",
                "lower_bound",
                "upper_bound",
            ] {
                let (mut schema, mut value, _) = fixture(version, false);
                let partition_schema = schema["fields"]
                    .as_array_mut()
                    .expect("fields")
                    .iter_mut()
                    .find(|field| field["name"] == "partitions")
                    .expect("partitions");
                let AvroValue::Record(fields) = &mut value else {
                    unreachable!("record")
                };
                let (_, AvroValue::Array(summaries)) = fields
                    .iter_mut()
                    .find(|(name, _)| name == "partitions")
                    .expect("partitions")
                else {
                    unreachable!("array")
                };
                replace_field(
                    &mut partition_schema["type"]["items"],
                    &mut summaries[0],
                    name,
                    Some((json!("string"), AvroValue::String("invalid".into()))),
                );
                assert_rejected(&schema, value);
            }
            let (mut schema, mut value, _) = fixture(version, false);
            let (empty_schema, empty_summary) = record("empty_summary", vec![]);
            replace_field(
                &mut schema,
                &mut value,
                "partitions",
                Some((
                    json!({"type": "array", "items": empty_schema}),
                    AvroValue::Array(vec![empty_summary]),
                )),
            );
            assert_rejected(&schema, value);
            for (ty, bad_value) in [
                (json!("int"), AvroValue::Int(3)),
                (
                    json!({"type": "array", "items": "int"}),
                    AvroValue::Array(vec![AvroValue::Int(3)]),
                ),
            ] {
                let (mut schema, mut value, _) = fixture(version, false);
                replace_field(&mut schema, &mut value, "partitions", Some((ty, bad_value)));
                assert_rejected(&schema, value);
            }
        }
    }

    #[test]
    fn write_context_headers_are_independent_of_entries_including_empty_lists() {
        for version in VERSIONS {
            for parent_snapshot_id in [None, Some(88)] {
                for empty in [false, true] {
                    let mut writer = ManifestListWriter::new();
                    if !empty {
                        writer.append(manifest_file());
                    }
                    let context = ManifestListWriteContext {
                        version,
                        snapshot_id: 999,
                        parent_snapshot_id,
                        sequence_number: if version == FormatVersion::V1 { 0 } else { 42 },
                        first_row_id: (version == FormatVersion::V3).then_some(123),
                    };
                    let bytes = writer
                        .to_bytes_with_context(context)
                        .expect("context write");
                    let reader =
                        AvroReader::new(bytes.as_slice()).expect("header even for empty list");
                    let meta = reader.user_metadata();
                    assert_eq!(
                        meta.get("format-version"),
                        Some(&(version as u8).to_string().into_bytes())
                    );
                    assert_eq!(meta.get("snapshot-id"), Some(&b"999".to_vec()));
                    assert_eq!(
                        meta.get("parent-snapshot-id"),
                        Some(
                            &parent_snapshot_id
                                .map_or_else(|| b"null".to_vec(), |id| id.to_string().into_bytes())
                        )
                    );
                    assert_eq!(
                        meta.get("sequence-number"),
                        (version >= FormatVersion::V2).then_some(&b"42".to_vec())
                    );
                    assert_eq!(
                        meta.get("first-row-id"),
                        (version == FormatVersion::V3).then_some(&b"123".to_vec())
                    );
                    assert_eq!(reader.count(), usize::from(!empty));
                    assert_eq!(
                        ManifestList::parse_with_version(&bytes, version)
                            .expect("decode list")
                            .entries
                            .len(),
                        usize::from(!empty)
                    );
                }
            }
        }
    }

    #[test]
    fn v1_reader_accepts_legacy_sail_v2_encoding() {
        let mut writer = ManifestListWriter::new();
        writer.append(manifest_file());
        let bytes = writer.to_bytes(FormatVersion::V2).expect("legacy bytes");

        let parsed = ManifestList::parse_with_version(&bytes, FormatVersion::V1)
            .expect("legacy Sail v1 manifest list");
        assert_eq!(parsed.entries, vec![manifest_file()]);
    }
}
