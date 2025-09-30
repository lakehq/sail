use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::partition::PartitionSpec;
use super::schema::SchemaRef;
use super::values::Literal;

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
}

/// Metadata about a manifest file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ManifestMetadata {
    /// The schema of the table when the manifest was written.
    pub schema: SchemaRef,
    /// The partition spec used to write the manifest.
    pub partition_spec: PartitionSpec,
    /// The format version of the manifest.
    pub format_version: FormatVersion,
}

impl ManifestMetadata {
    /// Create new manifest metadata.
    pub fn new(
        schema: SchemaRef,
        partition_spec: PartitionSpec,
        format_version: FormatVersion,
    ) -> Self {
        Self {
            schema,
            partition_spec,
            format_version,
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
    pub snapshot_id: i64,
    /// The sequence number when the data file was added to the table.
    pub sequence_number: i64,
    /// The data file.
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Create a new manifest entry.
    pub fn new(
        status: ManifestStatus,
        snapshot_id: i64,
        sequence_number: i64,
        data_file: DataFile,
    ) -> Self {
        Self {
            status,
            snapshot_id,
            sequence_number,
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
    pub lower_bounds: HashMap<i32, Literal>,
    /// Map from column id to upper bound in the column.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub upper_bounds: HashMap<i32, Literal>,
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
    /// The partition spec id used when writing this data file.
    pub partition_spec_id: i32,
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
    pub fn lower_bounds(&self) -> &HashMap<i32, Literal> {
        &self.lower_bounds
    }

    /// Get upper bounds.
    pub fn upper_bounds(&self) -> &HashMap<i32, Literal> {
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
    lower_bounds: HashMap<i32, Literal>,
    upper_bounds: HashMap<i32, Literal>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Vec<i64>,
    equality_ids: Vec<i32>,
    sort_order_id: Option<i32>,
    partition_spec_id: i32,
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
            key_metadata: None,
            split_offsets: Vec::new(),
            equality_ids: Vec::new(),
            sort_order_id: None,
            partition_spec_id: 0,
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
    pub fn with_lower_bound(mut self, column_id: i32, bound: Literal) -> Self {
        self.lower_bounds.insert(column_id, bound);
        self
    }

    /// Add upper bound.
    pub fn with_upper_bound(mut self, column_id: i32, bound: Literal) -> Self {
        self.upper_bounds.insert(column_id, bound);
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
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets,
            equality_ids: self.equality_ids,
            sort_order_id: self.sort_order_id,
            partition_spec_id: self.partition_spec_id,
        })
    }
}

impl Default for DataFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}
