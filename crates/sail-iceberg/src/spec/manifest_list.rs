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

use apache_avro::types::Value as AvroValue;
use apache_avro::{from_value as avro_from_value, Reader as AvroReader};
use serde::{Deserialize, Serialize};

use crate::spec::FormatVersion;
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

    /// Parse manifest list from bytes with a specified version.
    pub fn parse_with_version(bs: &[u8], version: FormatVersion) -> Result<ManifestList, String> {
        match version {
            FormatVersion::V1 => {
                let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
                let mut entries = Vec::new();
                for value in reader {
                    let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
                    let v1: _serde::ManifestFileV1 =
                        avro_from_value(&value).map_err(|e| format!("Avro decode error: {e}"))?;
                    entries.push(ManifestFile::from(v1));
                }
                Ok(ManifestList::new(entries))
            }
            FormatVersion::V2 => {
                let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
                let mut entries = Vec::new();
                for value in reader {
                    let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
                    match avro_from_value::<_serde::ManifestFileV2>(&value) {
                        Ok(v2) => entries.push(ManifestFile::from(v2)),
                        Err(_) => {
                            if let Ok(mf) = Self::parse_manifest_v2_fallback(&value) {
                                entries.push(mf);
                            } else {
                                let err = format!("Avro decode error: Failed to deserialize Avro value into value: {value:?}");
                                return Err(err);
                            }
                        }
                    }
                }
                Ok(ManifestList::new(entries))
            }
        }
    }
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

    pub fn to_bytes(&self, _version: FormatVersion) -> Result<Vec<u8>, String> {
        use apache_avro::Writer;

        use crate::spec::manifest_list::schema::MANIFEST_LIST_AVRO_SCHEMA_V2;

        // TODO: Implement typed V1 writer; currently only V2 is supported.
        let mut writer = Writer::new(&MANIFEST_LIST_AVRO_SCHEMA_V2, Vec::new());

        for mf in &self.entries {
            let v2 = _serde::ManifestFileV2 {
                manifest_path: mf.manifest_path.clone(),
                manifest_length: mf.manifest_length,
                partition_spec_id: mf.partition_spec_id,
                content: match mf.content {
                    ManifestContentType::Data => 0,
                    ManifestContentType::Deletes => 1,
                },
                sequence_number: mf.sequence_number,
                min_sequence_number: mf.min_sequence_number,
                added_snapshot_id: mf.added_snapshot_id,
                added_files_count: mf.added_files_count.unwrap_or(0),
                existing_files_count: mf.existing_files_count.unwrap_or(0),
                deleted_files_count: mf.deleted_files_count.unwrap_or(0),
                added_rows_count: mf.added_rows_count.unwrap_or(0),
                existing_rows_count: mf.existing_rows_count.unwrap_or(0),
                deleted_rows_count: mf.deleted_rows_count.unwrap_or(0),
                partitions: mf.partitions.clone().map(|ps| {
                    ps.into_iter()
                        .map(|p| FieldSummaryAvro {
                            contains_null: p.contains_null,
                            contains_nan: p.contains_nan,
                            lower_bound: p.lower_bound_bytes,
                            upper_bound: p.upper_bound_bytes,
                        })
                        .collect()
                }),
                key_metadata: mf.key_metadata.clone(),
            };
            // Enforce required fields for V2
            if mf.added_files_count.is_none()
                || mf.existing_files_count.is_none()
                || mf.deleted_files_count.is_none()
                || mf.added_rows_count.is_none()
                || mf.existing_rows_count.is_none()
                || mf.deleted_rows_count.is_none()
            {
                return Err("Missing required V2 counts/rows fields".to_string());
            }
            writer
                .append_ser(v2)
                .map_err(|e| format!("Avro append error: {e}"))?;
        }

        writer
            .into_inner()
            .map_err(|e| format!("Avro writer finalize error: {e}"))
    }
}

// removed duplicate early _serde block; see single _serde module below

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct FieldSummaryAvro {
    #[serde(rename = "contains_null")]
    contains_null: bool,
    #[serde(rename = "contains_nan")]
    contains_nan: Option<bool>,
    #[serde(rename = "lower_bound")]
    lower_bound: Option<Vec<u8>>,
    #[serde(rename = "upper_bound")]
    upper_bound: Option<Vec<u8>>,
}

impl From<FieldSummaryAvro> for FieldSummary {
    fn from(summary: FieldSummaryAvro) -> Self {
        let mut field_summary = FieldSummary::new(summary.contains_null);
        if let Some(contains_nan) = summary.contains_nan {
            field_summary = field_summary.with_contains_nan(contains_nan);
        }
        field_summary.lower_bound_bytes = summary.lower_bound;
        field_summary.upper_bound_bytes = summary.upper_bound;
        field_summary
    }
}

impl ManifestList {
    fn parse_manifest_v2_fallback(value: &AvroValue) -> Result<ManifestFile, String> {
        match value {
            AvroValue::Record(fields) => {
                let get = |name: &str| -> Option<&AvroValue> {
                    fields.iter().find(|(k, _)| k == name).map(|(_, v)| v)
                };

                let string = |v: &AvroValue| -> Result<String, String> {
                    if let AvroValue::String(s) = v {
                        Ok(s.clone())
                    } else {
                        Err("string".into())
                    }
                };
                let long = |v: &AvroValue| -> Result<i64, String> {
                    if let AvroValue::Long(x) = v {
                        Ok(*x)
                    } else {
                        Err("long".into())
                    }
                };
                let int = |v: &AvroValue| -> Result<i32, String> {
                    if let AvroValue::Int(x) = v {
                        Ok(*x)
                    } else {
                        Err("int".into())
                    }
                };

                let manifest_path = string(get("manifest_path").ok_or("manifest_path")?)?;
                let manifest_length = long(get("manifest_length").ok_or("manifest_length")?)?;
                let partition_spec_id = int(get("partition_spec_id").ok_or("partition_spec_id")?)?;
                let content = int(get("content").unwrap_or(&AvroValue::Int(0)))?;
                let sequence_number = long(get("sequence_number").ok_or("sequence_number")?)?;
                let min_sequence_number =
                    long(get("min_sequence_number").ok_or("min_sequence_number")?)?;
                let added_snapshot_id = long(get("added_snapshot_id").ok_or("added_snapshot_id")?)?;
                let added_files_count = get("added_files_count")
                    .or_else(|| get("added_data_files_count"))
                    .and_then(|v| {
                        if let AvroValue::Int(x) = v {
                            Some(*x)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let existing_files_count = get("existing_files_count")
                    .or_else(|| get("existing_data_files_count"))
                    .and_then(|v| {
                        if let AvroValue::Int(x) = v {
                            Some(*x)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let deleted_files_count = get("deleted_files_count")
                    .or_else(|| get("deleted_data_files_count"))
                    .and_then(|v| {
                        if let AvroValue::Int(x) = v {
                            Some(*x)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let added_rows_count = long(get("added_rows_count").ok_or("added_rows_count")?)?;
                let existing_rows_count =
                    long(get("existing_rows_count").ok_or("existing_rows_count")?)?;
                let deleted_rows_count =
                    long(get("deleted_rows_count").ok_or("deleted_rows_count")?)?;

                let partitions = match get("partitions") {
                    Some(AvroValue::Union(_, inner)) => match inner.as_ref() {
                        AvroValue::Array(items) => {
                            let mut out = Vec::new();
                            for it in items {
                                if let AvroValue::Record(fs) = it {
                                    let getf =
                                        |n: &str| fs.iter().find(|(k, _)| k == n).map(|(_, v)| v);
                                    let contains_null = matches!(getf("contains_null"), Some(AvroValue::Boolean(b)) if *b);
                                    let contains_nan = match getf("contains_nan") {
                                        Some(AvroValue::Boolean(b)) => Some(*b),
                                        _ => None,
                                    };
                                    let lower_bound_bytes = match getf("lower_bound") {
                                        Some(AvroValue::Bytes(b)) => Some(b.clone()),
                                        _ => None,
                                    };
                                    let upper_bound_bytes = match getf("upper_bound") {
                                        Some(AvroValue::Bytes(b)) => Some(b.clone()),
                                        _ => None,
                                    };
                                    let mut fs = FieldSummary::new(contains_null);
                                    if let Some(b) = contains_nan {
                                        fs = fs.with_contains_nan(b);
                                    }
                                    fs.lower_bound_bytes = lower_bound_bytes;
                                    fs.upper_bound_bytes = upper_bound_bytes;
                                    out.push(fs);
                                }
                            }
                            Some(out)
                        }
                        AvroValue::Null => None,
                        _ => None,
                    },
                    // Some writers may encode unexpected types; ignore invalid values
                    _ => None,
                };

                let key_metadata = match get("key_metadata") {
                    Some(AvroValue::Union(_, inner)) => match inner.as_ref() {
                        AvroValue::Bytes(b) => Some(b.clone()),
                        AvroValue::Null => None,
                        _ => None,
                    },
                    Some(AvroValue::Bytes(b)) => Some(b.clone()),
                    _ => None,
                };

                let content = match content {
                    0 => ManifestContentType::Data,
                    1 => ManifestContentType::Deletes,
                    _ => ManifestContentType::Data,
                };

                Ok(ManifestFile {
                    manifest_path,
                    manifest_length,
                    partition_spec_id,
                    content,
                    sequence_number,
                    min_sequence_number,
                    added_snapshot_id,
                    added_files_count: Some(added_files_count),
                    existing_files_count: Some(existing_files_count),
                    deleted_files_count: Some(deleted_files_count),
                    added_rows_count: Some(added_rows_count),
                    existing_rows_count: Some(existing_rows_count),
                    deleted_rows_count: Some(deleted_rows_count),
                    partitions,
                    key_metadata,
                })
            }
            _ => Err("not a record".into()),
        }
    }
}

impl From<_serde::ManifestFileV2> for ManifestFile {
    fn from(avro: _serde::ManifestFileV2) -> Self {
        let content = match avro.content {
            0 => ManifestContentType::Data,
            1 => ManifestContentType::Deletes,
            _ => ManifestContentType::Data,
        };

        let partitions = avro
            .partitions
            .map(|summaries| summaries.into_iter().map(FieldSummary::from).collect());

        ManifestFile {
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
            key_metadata: avro.key_metadata,
        }
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
    #[serde(rename_all = "kebab-case")]
    pub struct ManifestFileV1 {
        #[serde(rename = "manifest_path")]
        pub manifest_path: String,
        #[serde(rename = "manifest_length")]
        pub manifest_length: i64,
        #[serde(rename = "partition_spec_id")]
        pub partition_spec_id: i32,
        #[serde(rename = "added_snapshot_id")]
        pub added_snapshot_id: i64,
        #[serde(rename = "added_data_files_count")]
        pub added_data_files_count: Option<i32>,
        #[serde(rename = "existing_data_files_count")]
        pub existing_data_files_count: Option<i32>,
        #[serde(rename = "deleted_data_files_count")]
        pub deleted_data_files_count: Option<i32>,
        #[serde(rename = "added_rows_count")]
        pub added_rows_count: Option<i64>,
        #[serde(rename = "existing_rows_count")]
        pub existing_rows_count: Option<i64>,
        #[serde(rename = "deleted_rows_count")]
        pub deleted_rows_count: Option<i64>,
        #[serde(rename = "partitions")]
        pub partitions: Option<Vec<FieldSummaryAvro>>, // V1 uses same summary encoding
        #[serde(rename = "key_metadata")]
        pub key_metadata: Option<Vec<u8>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub struct ManifestFileV2 {
        #[serde(rename = "manifest_path")]
        pub manifest_path: String,
        #[serde(rename = "manifest_length")]
        pub manifest_length: i64,
        #[serde(rename = "partition_spec_id")]
        pub partition_spec_id: i32,
        #[serde(rename = "content")]
        pub content: i32,
        #[serde(rename = "sequence_number")]
        pub sequence_number: i64,
        #[serde(rename = "min_sequence_number")]
        pub min_sequence_number: i64,
        #[serde(rename = "added_snapshot_id")]
        pub added_snapshot_id: i64,
        #[serde(rename = "added_files_count", alias = "added_data_files_count")]
        pub added_files_count: i32,
        #[serde(rename = "existing_files_count", alias = "existing_data_files_count")]
        pub existing_files_count: i32,
        #[serde(rename = "deleted_files_count", alias = "deleted_data_files_count")]
        pub deleted_files_count: i32,
        #[serde(rename = "added_rows_count")]
        pub added_rows_count: i64,
        #[serde(rename = "existing_rows_count")]
        pub existing_rows_count: i64,
        #[serde(rename = "deleted_rows_count")]
        pub deleted_rows_count: i64,
        #[serde(rename = "partitions")]
        pub partitions: Option<Vec<FieldSummaryAvro>>,
        #[serde(rename = "key_metadata")]
        pub key_metadata: Option<Vec<u8>>,
    }
}

impl From<_serde::ManifestFileV1> for ManifestFile {
    fn from(v1: _serde::ManifestFileV1) -> Self {
        ManifestFile {
            manifest_path: v1.manifest_path,
            manifest_length: v1.manifest_length,
            partition_spec_id: v1.partition_spec_id,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: v1.added_snapshot_id,
            added_files_count: v1.added_data_files_count,
            existing_files_count: v1.existing_data_files_count,
            deleted_files_count: v1.deleted_data_files_count,
            added_rows_count: v1.added_rows_count,
            existing_rows_count: v1.existing_rows_count,
            deleted_rows_count: v1.deleted_rows_count,
            partitions: v1
                .partitions
                .map(|v| v.into_iter().map(FieldSummary::from).collect()),
            key_metadata: v1.key_metadata,
        }
    }
}
