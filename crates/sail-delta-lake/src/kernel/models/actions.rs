use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::str::FromStr;

pub use delta_kernel::actions::Metadata;
use delta_kernel::schema::{DataType, StructField};
use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use deltalake::kernel::{DeltaResult, Error, StructType, StructTypeExt};
use deltalake::TableProperty;
use serde::{Deserialize, Serialize};

pub fn new_metadata(
    schema: &StructType,
    partition_columns: impl IntoIterator<Item = impl ToString>,
    configuration: impl IntoIterator<Item = (impl ToString, impl ToString)>,
) -> DeltaResult<Metadata> {
    let value = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "name": None::<String>,
        "description": None::<String>,
        "format": { "provider": "parquet", "options": {} },
        "schemaString": serde_json::to_string(schema)?,
        "partitionColumns": partition_columns.into_iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "configuration": configuration.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<HashMap<_, _>>(),
        "createdTime": chrono::Utc::now().timestamp_millis(),
    });
    Ok(serde_json::from_value(value)?)
}

#[allow(dead_code)]
pub trait MetadataExt {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata>;

    fn with_name(self, name: String) -> DeltaResult<Metadata>;

    fn with_description(self, description: String) -> DeltaResult<Metadata>;

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata>;

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata>;

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata>;
}

impl MetadataExt for Metadata {
    fn with_table_id(self, table_id: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": table_id,
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_name(self, name: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": name,
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_description(self, description: String) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": description,
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn with_schema(self, schema: &StructType) -> DeltaResult<Metadata> {
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(schema)?,
            "partitionColumns": self.partition_columns(),
            "configuration": self.configuration(),
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn add_config_key(self, key: String, value: String) -> DeltaResult<Metadata> {
        let mut config = self.configuration().clone();
        config.insert(key, value);
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": config,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }

    fn remove_config_key(self, key: &str) -> DeltaResult<Metadata> {
        let mut config = self.configuration().clone();
        config.remove(key);
        let value = serde_json::json!({
            "id": self.id(),
            "name": self.name(),
            "description": self.description(),
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(&self.parse_schema().unwrap())?,
            "partitionColumns": self.partition_columns(),
            "configuration": config,
            "createdTime": self.created_time(),
        });
        Ok(serde_json::from_value(value)?)
    }
}

/// checks if table contains timestamp_ntz in any field including nested fields.
pub fn contains_timestampntz<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fn _check_type(dtype: &DataType) -> bool {
        match dtype {
            &DataType::TIMESTAMP_NTZ => true,
            DataType::Array(inner) => _check_type(inner.element_type()),
            DataType::Struct(inner) => inner.fields().any(|f| _check_type(f.data_type())),
            _ => false,
        }
    }
    fields.any(|f| _check_type(f.data_type()))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Defines a protocol action
pub struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    pub min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    pub min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_features: Option<HashSet<ReaderFeature>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer_features: Option<HashSet<WriterFeature>>,
}

impl Default for Protocol {
    fn default() -> Self {
        Self {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        }
    }
}

#[allow(dead_code)]
impl Protocol {
    /// Create a new protocol action
    pub fn new(min_reader_version: i32, min_writer_version: i32) -> Self {
        Self {
            min_reader_version,
            min_writer_version,
            reader_features: None,
            writer_features: None,
        }
    }

    /// Append the reader features in the protocol action, automatically bumps min_reader_version
    pub fn append_reader_features(
        mut self,
        reader_features: impl IntoIterator<Item = impl Into<ReaderFeature>>,
    ) -> Self {
        let all_reader_features = reader_features
            .into_iter()
            .map(Into::into)
            .collect::<HashSet<_>>();
        if !all_reader_features.is_empty() {
            self.min_reader_version = 3;
            match self.reader_features {
                Some(mut features) => {
                    features.extend(all_reader_features);
                    self.reader_features = Some(features);
                }
                None => self.reader_features = Some(all_reader_features),
            };
        }
        self
    }

    /// Append the writer features in the protocol action, automatically bumps min_writer_version
    pub fn append_writer_features(
        mut self,
        writer_features: impl IntoIterator<Item = impl Into<WriterFeature>>,
    ) -> Self {
        let all_writer_features = writer_features
            .into_iter()
            .map(|c| c.into())
            .collect::<HashSet<_>>();
        if !all_writer_features.is_empty() {
            self.min_writer_version = 7;

            match self.writer_features {
                Some(mut features) => {
                    features.extend(all_writer_features);
                    self.writer_features = Some(features);
                }
                None => self.writer_features = Some(all_writer_features),
            };
        }
        self
    }

    /// Converts existing properties into features if the reader_version is >=3 or writer_version >=7
    /// only converts features that are "true"
    pub fn move_table_properties_into_features(
        mut self,
        configuration: &HashMap<String, String>,
    ) -> Protocol {
        fn parse_bool(value: &str) -> bool {
            value.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v)
        }

        if self.min_writer_version >= 7 {
            // TODO: move this is in future to use delta_kernel::table_properties
            let mut converted_writer_features = configuration
                .iter()
                .filter(|(_, value)| value.to_ascii_lowercase().parse::<bool>().is_ok_and(|v| v))
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableChangeDataFeed" if parse_bool(value) => {
                        Some(WriterFeature::ChangeDataFeed)
                    }
                    "delta.appendOnly" if parse_bool(value) => Some(WriterFeature::AppendOnly),
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(WriterFeature::DeletionVectors)
                    }
                    "delta.enableRowTracking" if parse_bool(value) => {
                        Some(WriterFeature::RowTracking)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(WriterFeature::V2Checkpoint),
                    _ => None,
                })
                .collect::<HashSet<WriterFeature>>();

            if configuration
                .keys()
                .any(|v| v.starts_with("delta.constraints."))
            {
                converted_writer_features.insert(WriterFeature::CheckConstraints);
            }

            match self.writer_features {
                Some(mut features) => {
                    features.extend(converted_writer_features);
                    self.writer_features = Some(features);
                }
                None => self.writer_features = Some(converted_writer_features),
            }
        }
        if self.min_reader_version >= 3 {
            let converted_reader_features = configuration
                .iter()
                .filter_map(|(key, value)| match key.as_str() {
                    "delta.enableDeletionVectors" if parse_bool(value) => {
                        Some(ReaderFeature::DeletionVectors)
                    }
                    "delta.checkpointPolicy" if value == "v2" => Some(ReaderFeature::V2Checkpoint),
                    _ => None,
                })
                .collect::<HashSet<ReaderFeature>>();
            match self.reader_features {
                Some(mut features) => {
                    features.extend(converted_reader_features);
                    self.reader_features = Some(features);
                }
                None => self.reader_features = Some(converted_reader_features),
            }
        }
        self
    }

    /// Will apply the column metadata to the protocol by either bumping the version or setting
    /// features
    pub fn apply_column_metadata_to_protocol(
        mut self,
        schema: &StructType,
    ) -> DeltaResult<Protocol> {
        let generated_cols = schema.get_generated_columns()?;
        let invariants = schema.get_invariants()?;
        let contains_timestamp_ntz = self.contains_timestampntz(schema.fields());

        if contains_timestamp_ntz {
            self = self.enable_timestamp_ntz()
        }

        if !generated_cols.is_empty() {
            self = self.enable_generated_columns()
        }

        if !invariants.is_empty() {
            self = self.enable_invariants()
        }

        Ok(self)
    }

    /// Will apply the properties to the protocol by either bumping the version or setting
    /// features
    pub fn apply_properties_to_protocol(
        mut self,
        new_properties: &HashMap<String, String>,
        raise_if_not_exists: bool,
    ) -> DeltaResult<Protocol> {
        let mut parsed_properties: HashMap<TableProperty, String> = HashMap::new();

        for (key, value) in new_properties {
            if let Ok(parsed_key) = key.parse::<TableProperty>() {
                parsed_properties.insert(parsed_key, value.to_string());
            } else if raise_if_not_exists {
                return Err(Error::Generic(format!(
                    "Error parsing property '{key}':'{value}'",
                )));
            }
        }

        // Check and update delta.minReaderVersion
        if let Some(min_reader_version) = parsed_properties.get(&TableProperty::MinReaderVersion) {
            let new_min_reader_version = min_reader_version.parse::<i32>();
            match new_min_reader_version {
                Ok(version) => match version {
                    1..=3 => {
                        if version > self.min_reader_version {
                            self.min_reader_version = version
                        }
                    }
                    _ => {
                        return Err(Error::Generic(format!("delta.minReaderVersion = '{min_reader_version}' is invalid, valid values are ['1','2','3']")))
                    }
                },
                Err(_) => {
                    return Err(Error::Generic(format!("delta.minReaderVersion = '{min_reader_version}' is invalid, valid values are ['1','2','3']")))
                }
            }
        }

        // Check and update delta.minWriterVersion
        if let Some(min_writer_version) = parsed_properties.get(&TableProperty::MinWriterVersion) {
            let new_min_writer_version = min_writer_version.parse::<i32>();
            match new_min_writer_version {
                Ok(version) => match version {
                    2..=7 => {
                        if version > self.min_writer_version {
                            self.min_writer_version = version
                        }
                    }
                    _ => {
                        return Err(Error::Generic(format!("delta.minWriterVersion = '{min_writer_version}' is invalid, valid values are ['2','3','4','5','6','7']")))
                    }
                },
                Err(_) => {
                    return Err(Error::Generic(format!("delta.minWriterVersion = '{min_writer_version}' is invalid, valid values are ['2','3','4','5','6','7']")))
                }
            }
        }

        // Check enableChangeDataFeed and bump protocol or add writerFeature if writer versions is >=7
        if let Some(enable_cdf) = parsed_properties.get(&TableProperty::EnableChangeDataFeed) {
            let if_enable_cdf = enable_cdf.to_ascii_lowercase().parse::<bool>();
            match if_enable_cdf {
                Ok(true) => {
                    if self.min_writer_version >= 7 {
                        match self.writer_features {
                            Some(mut features) => {
                                features.insert(WriterFeature::ChangeDataFeed);
                                self.writer_features = Some(features);
                            }
                            None => {
                                self.writer_features =
                                    Some(HashSet::from([WriterFeature::ChangeDataFeed]))
                            }
                        }
                    } else if self.min_writer_version <= 3 {
                        self.min_writer_version = 4
                    }
                }
                Ok(false) => {}
                _ => {
                    return Err(Error::Generic(format!("delta.enableChangeDataFeed = '{enable_cdf}' is invalid, valid values are ['true']")))
                }
            }
        }

        if let Some(enable_dv) = parsed_properties.get(&TableProperty::EnableDeletionVectors) {
            let if_enable_dv = enable_dv.to_ascii_lowercase().parse::<bool>();
            match if_enable_dv {
                Ok(true) => {
                    let writer_features = match self.writer_features {
                        Some(mut features) => {
                            features.insert(WriterFeature::DeletionVectors);
                            features
                        }
                        None => HashSet::from([WriterFeature::DeletionVectors]),
                    };
                    let reader_features = match self.reader_features {
                        Some(mut features) => {
                            features.insert(ReaderFeature::DeletionVectors);
                            features
                        }
                        None => HashSet::from([ReaderFeature::DeletionVectors]),
                    };
                    self.min_reader_version = 3;
                    self.min_writer_version = 7;
                    self.writer_features = Some(writer_features);
                    self.reader_features = Some(reader_features);
                }
                Ok(false) => {}
                _ => {
                    return Err(Error::Generic(format!("delta.enableDeletionVectors = '{enable_dv}' is invalid, valid values are ['true']")))
                }
            }
        }
        Ok(self)
    }

    /// checks if table contains timestamp_ntz in any field including nested fields.
    fn contains_timestampntz<'a>(&self, fields: impl Iterator<Item = &'a StructField>) -> bool {
        contains_timestampntz(fields)
    }

    /// Enable timestamp_ntz in the protocol
    fn enable_timestamp_ntz(mut self) -> Self {
        self = self.append_reader_features([ReaderFeature::TimestampWithoutTimezone]);
        self = self.append_writer_features([WriterFeature::TimestampWithoutTimezone]);
        self
    }

    /// Enabled generated columns
    fn enable_generated_columns(mut self) -> Self {
        if self.min_writer_version < 4 {
            self.min_writer_version = 4;
        }
        if self.min_writer_version >= 7 {
            self = self.append_writer_features([WriterFeature::GeneratedColumns]);
        }
        self
    }

    /// Enabled generated columns
    fn enable_invariants(mut self) -> Self {
        if self.min_writer_version >= 7 {
            self = self.append_writer_features([WriterFeature::Invariants]);
        }
        self
    }
}

/// High level table features
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TableFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// ID Columns
    IdentityColumns,
    /// Row tracking on tables
    RowTracking,
    /// domain specific metadata
    DomainMetadata,
    /// Iceberg compatibility support
    IcebergCompatV1,
}

impl FromStr for TableFeatures {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "columnMapping" => Ok(TableFeatures::ColumnMapping),
            "deletionVectors" => Ok(TableFeatures::DeletionVectors),
            "timestampNtz" => Ok(TableFeatures::TimestampWithoutTimezone),
            "v2Checkpoint" => Ok(TableFeatures::V2Checkpoint),
            "appendOnly" => Ok(TableFeatures::AppendOnly),
            "invariants" => Ok(TableFeatures::Invariants),
            "checkConstraints" => Ok(TableFeatures::CheckConstraints),
            "changeDataFeed" => Ok(TableFeatures::ChangeDataFeed),
            "generatedColumns" => Ok(TableFeatures::GeneratedColumns),
            "identityColumns" => Ok(TableFeatures::IdentityColumns),
            "rowTracking" => Ok(TableFeatures::RowTracking),
            "domainMetadata" => Ok(TableFeatures::DomainMetadata),
            "icebergCompatV1" => Ok(TableFeatures::IcebergCompatV1),
            _ => Err(()),
        }
    }
}

impl AsRef<str> for TableFeatures {
    fn as_ref(&self) -> &str {
        match self {
            TableFeatures::ColumnMapping => "columnMapping",
            TableFeatures::DeletionVectors => "deletionVectors",
            TableFeatures::TimestampWithoutTimezone => "timestampNtz",
            TableFeatures::V2Checkpoint => "v2Checkpoint",
            TableFeatures::AppendOnly => "appendOnly",
            TableFeatures::Invariants => "invariants",
            TableFeatures::CheckConstraints => "checkConstraints",
            TableFeatures::ChangeDataFeed => "changeDataFeed",
            TableFeatures::GeneratedColumns => "generatedColumns",
            TableFeatures::IdentityColumns => "identityColumns",
            TableFeatures::RowTracking => "rowTracking",
            TableFeatures::DomainMetadata => "domainMetadata",
            TableFeatures::IcebergCompatV1 => "icebergCompatV1",
        }
    }
}

impl fmt::Display for TableFeatures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl TryFrom<&TableFeatures> for ReaderFeature {
    type Error = String;

    fn try_from(_value: &TableFeatures) -> Result<Self, Self::Error> {
        // TODO: Implement proper conversion
        Err("Not implemented".to_string())
    }
}

impl TryFrom<&TableFeatures> for WriterFeature {
    type Error = String;

    fn try_from(_value: &TableFeatures) -> Result<Self, Self::Error> {
        // TODO: Implement proper conversion
        Err("Not implemented".to_string())
    }
}

impl TableFeatures {
    /// Convert table feature to respective reader or/and write feature
    pub fn to_reader_writer_features(&self) -> (Option<ReaderFeature>, Option<WriterFeature>) {
        let reader_feature = ReaderFeature::try_from(self)
            .ok()
            .and_then(|feature| match feature {
                ReaderFeature::Unknown(_) => None,
                _ => Some(feature),
            });
        let writer_feature = WriterFeature::try_from(self)
            .ok()
            .and_then(|feature| match feature {
                WriterFeature::Unknown(_) => None,
                _ => Some(feature),
            });
        (reader_feature, writer_feature)
    }
}

///Storage type of deletion vector
#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum StorageType {
    /// Stored at relative path derived from a UUID.
    #[serde(rename = "u")]
    UuidRelativePath,
    /// Stored as inline string.
    #[serde(rename = "i")]
    Inline,
    /// Stored at an absolute path.
    #[serde(rename = "p")]
    AbsolutePath,
}

impl Default for StorageType {
    fn default() -> Self {
        Self::UuidRelativePath // seems to be used by Databricks and therefore most common
    }
}

impl FromStr for StorageType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u" => Ok(Self::UuidRelativePath),
            "i" => Ok(Self::Inline),
            "p" => Ok(Self::AbsolutePath),
            _ => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{s}'."
            ))),
        }
    }
}

impl AsRef<str> for StorageType {
    fn as_ref(&self) -> &str {
        match self {
            Self::UuidRelativePath => "u",
            Self::Inline => "i",
            Self::AbsolutePath => "p",
        }
    }
}

impl Display for StorageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: StorageType,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
/// Defines an add action
pub struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    #[serde(with = "serde_path")]
    pub path: String,

    /// A map from partition column to value for this logical file.
    pub partition_values: HashMap<String, Option<String>>,

    /// The size of this data file in bytes
    pub size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// Information about deletion vector (DV) associated with this add action
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    pub clustering_provider: Option<String>,
}

/// Represents a tombstone (deleted file) in the Delta log.
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, Eq, Default)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    #[serde(with = "serde_path")]
    pub path: String,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub data_change: bool,

    /// The time this logical file was created, as milliseconds since the epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, Option<String>>>,

    /// The size of this data file in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,

    /// Map containing metadata about this logical file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,

    /// Information about deletion vector (DV) associated with this add action
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_row_commit_version: Option<i64>,
}

/// Delta AddCDCFile action that describes a parquet CDC data file.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AddCDCFile {
    /// A relative path, from the root of the table, or an
    /// absolute path to a CDC file
    #[serde(with = "serde_path")]
    pub path: String,

    /// The size of this file in bytes
    pub size: i64,

    /// A map from partition column to value for this file
    pub partition_values: HashMap<String, Option<String>>,

    /// Should always be set to false because they do not change the underlying data of the table
    pub data_change: bool,

    /// Map containing metadata about this file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// Action used by streaming systems to track progress using application-specific versions to
/// enable idempotency.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    /// A unique identifier for the application performing the transaction.
    pub app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<i64>,
}

#[allow(dead_code)]
impl Transaction {
    /// Create a new application transactions. See [`Txn`] for details.
    pub fn new(app_id: impl ToString, version: i64) -> Self {
        Self::new_with_last_update(app_id, version, None)
    }

    /// Create a new application transactions. See [`Txn`] for details.
    pub fn new_with_last_update(
        app_id: impl ToString,
        version: i64,
        last_updated: Option<i64>,
    ) -> Self {
        Transaction {
            app_id: app_id.to_string(),
            version,
            last_updated,
        }
    }
}

/// The commitInfo is a fairly flexible action within the delta specification, where arbitrary data can be stored.
/// However the reference implementation as well as delta-rs store useful information that may for instance
/// allow us to be more permissive in commit conflict resolution.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct CommitInfo {
    /// Timestamp in millis when the commit was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,

    /// Id of the user invoking the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Name of the user invoking the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,

    /// The operation performed during the
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,

    /// Parameters used for table operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_parameters: Option<HashMap<String, serde_json::Value>>,

    /// Version of the table when the operation was started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_version: Option<i64>,

    /// The isolation level of the commit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,

    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_blind_append: Option<bool>,

    /// Delta engine which created the commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine_info: Option<String>,

    /// Additional provenance information for the commit
    #[serde(flatten, default)]
    pub info: HashMap<String, serde_json::Value>,

    /// User defined metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_metadata: Option<String>,
}

/// The domain metadata action contains a configuration (string) for a named metadata domain
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DomainMetadata {
    /// Identifier for this domain (system or user-provided)
    pub domain: String,

    /// String containing configuration for the metadata domain
    pub configuration: String,

    /// When `true` the action serves as a tombstone
    pub removed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[allow(dead_code)]
/// This action is only allowed in checkpoints following V2 spec. It describes the details about the checkpoint.
pub struct CheckpointMetadata {
    /// The flavor of the V2 checkpoint. Allowed values: "flat".
    pub flavor: String,

    /// Map containing any additional metadata about the v2 spec checkpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's file actions.
/// This action is only allowed in checkpoints following V2 spec.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Sidecar {
    /// The name of the sidecar file (not a path).
    /// The file must reside in the _delta_log/_sidecars directory.
    pub file_name: String,

    /// The size of the sidecar file in bytes
    pub size_in_bytes: i64,

    /// The time this sidecar file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// Type of sidecar. Valid values are: "fileaction".
    /// This could be extended in future to allow different kinds of sidecars.
    #[serde(rename = "type")]
    pub sidecar_type: String,

    /// Map containing any additional metadata about the checkpoint sidecar file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, Option<String>>>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
/// The isolation level applied during transaction
pub enum IsolationLevel {
    /// The strongest isolation level. It ensures that committed write operations
    /// and all reads are Serializable. Operations are allowed as long as there
    /// exists a serial sequence of executing them one-at-a-time that generates
    /// the same outcome as that seen in the table. For the write operations,
    /// the serial sequence is exactly the same as that seen in the table’s history.
    Serializable,

    /// A weaker isolation level than Serializable. It ensures only that the write
    /// operations (that is, not reads) are serializable. However, this is still stronger
    /// than Snapshot isolation. WriteSerializable is the default isolation level because
    /// it provides great balance of data consistency and availability for most common operations.
    WriteSerializable,

    /// SnapshotIsolation is a guarantee that all reads made in a transaction will see a consistent
    /// snapshot of the database (in practice it reads the last committed values that existed at the
    /// time it started), and the transaction itself will successfully commit only if no updates
    /// it has made conflict with any concurrent updates made since that snapshot.
    SnapshotIsolation,
}

// Spark assumes Serializable as default isolation level
// https://github.com/delta-io/delta/blob/abb171c8401200e7772b27e3be6ea8682528ac72/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1023
impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Serializable
    }
}

impl AsRef<str> for IsolationLevel {
    fn as_ref(&self) -> &str {
        match self {
            Self::Serializable => "Serializable",
            Self::WriteSerializable => "WriteSerializable",
            Self::SnapshotIsolation => "SnapshotIsolation",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "serializable" => Ok(Self::Serializable),
            "writeserializable" | "write_serializable" => Ok(Self::WriteSerializable),
            "snapshotisolation" | "snapshot_isolation" => Ok(Self::SnapshotIsolation),
            _ => Err(Error::Generic("Invalid string for IsolationLevel".into())),
        }
    }
}

#[allow(dead_code)]
pub(crate) mod serde_path {
    use std::str::Utf8Error;

    use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        decode_path(&s).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = encode_path(value);
        String::serialize(&encoded, serializer)
    }

    pub const _DELIMITER: &str = "/";
    /// The path delimiter as a single byte
    pub const _DELIMITER_BYTE: u8 = _DELIMITER.as_bytes()[0];

    /// Characters we want to encode.
    const INVALID: &AsciiSet = &CONTROLS
        // The delimiter we are reserving for internal hierarchy
        // .add(DELIMITER_BYTE)
        // Characters AWS recommends avoiding for object keys
        // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
        .add(b'\\')
        .add(b'{')
        .add(b'^')
        .add(b'}')
        .add(b'%')
        .add(b'`')
        .add(b']')
        .add(b'"')
        .add(b'>')
        .add(b'[')
        // .add(b'~')
        .add(b'<')
        .add(b'#')
        .add(b'|')
        // Characters Google Cloud Storage recommends avoiding for object names
        // https://cloud.google.com/storage/docs/naming-objects
        .add(b'\r')
        .add(b'\n')
        .add(b'*')
        .add(b'?');

    fn encode_path(path: &str) -> String {
        percent_encode(path.as_bytes(), INVALID).to_string()
    }

    pub fn decode_path(path: &str) -> Result<String, Utf8Error> {
        Ok(percent_decode_str(path).decode_utf8()?.to_string())
    }
}
