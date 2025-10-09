use apache_avro::{from_value as avro_from_value, Reader as AvroReader};
use serde::{Deserialize, Serialize};

use super::values::{Literal, PrimitiveLiteral};

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
    pub fn parse_with_version(
        bs: &[u8],
        _version: super::FormatVersion,
    ) -> Result<ManifestList, String> {
        // Decode per-record to avoid array-level serde issues; field aliases/defaults cover V1/V2
        let reader = AvroReader::new(bs).map_err(|e| format!("Avro read error: {e}"))?;
        let mut manifest_files = Vec::new();
        for value in reader {
            let value = value.map_err(|e| format!("Avro read value error: {e}"))?;
            let mf_avro: ManifestFileAvro =
                avro_from_value(&value).map_err(|e| format!("Avro decode error: {e}"))?;
            manifest_files.push(mf_avro.into());
        }
        Ok(ManifestList::new(manifest_files))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ManifestFileAvro {
    #[serde(rename = "manifest_path")]
    manifest_path: String,
    #[serde(rename = "manifest_length")]
    manifest_length: i64,
    #[serde(rename = "partition_spec_id")]
    partition_spec_id: i32,
    #[serde(rename = "content")]
    content: i32,
    #[serde(rename = "sequence_number")]
    sequence_number: i64,
    #[serde(rename = "min_sequence_number")]
    min_sequence_number: i64,
    #[serde(rename = "added_snapshot_id")]
    added_snapshot_id: i64,
    #[serde(alias = "added_data_files_count", rename = "added_files_count")]
    added_files_count: i32,
    #[serde(alias = "existing_data_files_count", rename = "existing_files_count")]
    existing_files_count: i32,
    #[serde(alias = "deleted_data_files_count", rename = "deleted_files_count")]
    deleted_files_count: i32,
    #[serde(rename = "added_rows_count")]
    added_rows_count: i64,
    #[serde(rename = "existing_rows_count")]
    existing_rows_count: i64,
    #[serde(rename = "deleted_rows_count")]
    deleted_rows_count: i64,
    #[serde(rename = "partitions")]
    partitions: Option<Vec<FieldSummaryAvro>>,
    #[serde(rename = "key_metadata")]
    key_metadata: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct FieldSummaryAvro {
    #[serde(rename = "contains_null")]
    contains_null: bool,
    #[serde(rename = "contains_nan")]
    contains_nan: Option<bool>,
    #[serde(rename = "lower_bound")]
    lower_bound: Option<Vec<u8>>,
    #[serde(rename = "upper_bound")]
    upper_bound: Option<Vec<u8>>,
}

impl From<ManifestFileAvro> for ManifestFile {
    fn from(avro: ManifestFileAvro) -> Self {
        let content = match avro.content {
            0 => ManifestContentType::Data,
            1 => ManifestContentType::Deletes,
            _ => ManifestContentType::Data,
        };

        let partitions = avro.partitions.map(|summaries| {
            summaries
                .into_iter()
                .map(|summary| {
                    let lower_bound = summary
                        .lower_bound
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                        .map(|s| Literal::Primitive(PrimitiveLiteral::String(s)));

                    let upper_bound = summary
                        .upper_bound
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                        .map(|s| Literal::Primitive(PrimitiveLiteral::String(s)));

                    let mut field_summary = FieldSummary::new(summary.contains_null);
                    if let Some(contains_nan) = summary.contains_nan {
                        field_summary = field_summary.with_contains_nan(contains_nan);
                    }
                    if let Some(lower) = lower_bound {
                        field_summary = field_summary.with_lower_bound(lower);
                    }
                    if let Some(upper) = upper_bound {
                        field_summary = field_summary.with_upper_bound(upper);
                    }
                    field_summary
                })
                .collect()
        });

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
    /// The minimum value of the partition field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Literal>,
    /// The maximum value of the partition field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Literal>,
}

impl FieldSummary {
    /// Create a new field summary.
    pub fn new(contains_null: bool) -> Self {
        Self {
            contains_null,
            contains_nan: None,
            lower_bound: None,
            upper_bound: None,
        }
    }

    /// Set whether the field contains NaN values.
    pub fn with_contains_nan(mut self, contains_nan: bool) -> Self {
        self.contains_nan = Some(contains_nan);
        self
    }

    /// Set the lower bound of the field.
    pub fn with_lower_bound(mut self, lower_bound: Literal) -> Self {
        self.lower_bound = Some(lower_bound);
        self
    }

    /// Set the upper bound of the field.
    pub fn with_upper_bound(mut self, upper_bound: Literal) -> Self {
        self.upper_bound = Some(upper_bound);
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
    #[serde(transparent)]
    pub(crate) struct ManifestListV1 {
        entries: Vec<ManifestFileV1>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct ManifestListV2 {
        entries: Vec<ManifestFileV2>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ManifestFileV1 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        pub added_snapshot_id: i64,
        pub added_data_files_count: Option<i32>,
        pub existing_data_files_count: Option<i32>,
        pub deleted_data_files_count: Option<i32>,
        pub added_rows_count: Option<i64>,
        pub existing_rows_count: Option<i64>,
        pub deleted_rows_count: Option<i64>,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<Vec<u8>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) struct ManifestFileV2 {
        pub manifest_path: String,
        pub manifest_length: i64,
        pub partition_spec_id: i32,
        #[serde(default = "v2_default_content_for_v1")]
        pub content: i32,
        #[serde(default = "v2_default_sequence_number_for_v1")]
        pub sequence_number: i64,
        #[serde(default = "v2_default_min_sequence_number_for_v1")]
        pub min_sequence_number: i64,
        #[serde(alias = "added_data_files_count", alias = "added_files_count")]
        pub added_files_count: i32,
        #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
        pub existing_files_count: i32,
        #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
        pub deleted_files_count: i32,
        pub added_snapshot_id: i64,
        pub added_rows_count: i64,
        pub existing_rows_count: i64,
        pub deleted_rows_count: i64,
        pub partitions: Option<Vec<FieldSummary>>,
        pub key_metadata: Option<Vec<u8>>,
    }

    const fn v2_default_content_for_v1() -> i32 {
        super::ManifestContentType::Data as i32
    }
    const fn v2_default_sequence_number_for_v1() -> i64 {
        0
    }
    const fn v2_default_min_sequence_number_for_v1() -> i64 {
        0
    }

    impl TryFrom<ManifestListV1> for super::ManifestList {
        type Error = String;
        fn try_from(v1: ManifestListV1) -> Result<Self, Self::Error> {
            let entries = v1
                .entries
                .into_iter()
                .map(|e| ManifestFile {
                    manifest_path: e.manifest_path,
                    manifest_length: e.manifest_length,
                    partition_spec_id: e.partition_spec_id,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: e.added_snapshot_id,
                    added_files_count: e.added_data_files_count,
                    existing_files_count: e.existing_data_files_count,
                    deleted_files_count: e.deleted_data_files_count,
                    added_rows_count: e.added_rows_count,
                    existing_rows_count: e.existing_rows_count,
                    deleted_rows_count: e.deleted_rows_count,
                    partitions: e.partitions,
                    key_metadata: e.key_metadata,
                })
                .collect();
            Ok(super::ManifestList::new(entries))
        }
    }

    impl TryFrom<ManifestListV2> for super::ManifestList {
        type Error = String;
        fn try_from(v2: ManifestListV2) -> Result<Self, Self::Error> {
            let entries = v2
                .entries
                .into_iter()
                .map(|e| ManifestFile {
                    manifest_path: e.manifest_path,
                    manifest_length: e.manifest_length,
                    partition_spec_id: e.partition_spec_id,
                    content: match e.content {
                        1 => ManifestContentType::Deletes,
                        _ => ManifestContentType::Data,
                    },
                    sequence_number: e.sequence_number,
                    min_sequence_number: e.min_sequence_number,
                    added_snapshot_id: e.added_snapshot_id,
                    added_files_count: Some(e.added_files_count),
                    existing_files_count: Some(e.existing_files_count),
                    deleted_files_count: Some(e.deleted_files_count),
                    added_rows_count: Some(e.added_rows_count),
                    existing_rows_count: Some(e.existing_rows_count),
                    deleted_rows_count: Some(e.deleted_rows_count),
                    partitions: e.partitions,
                    key_metadata: e.key_metadata,
                })
                .collect();
            Ok(super::ManifestList::new(entries))
        }
    }
}
