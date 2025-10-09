use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::{from_value, Reader as AvroReader};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::Result as DataFusionResult;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path as ObjectPath;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::arrow_conversion::iceberg_schema_to_arrow;
use crate::spec::{
    DataContentType, DataFile, FieldSummary, Literal, ManifestContentType, ManifestFile,
    ManifestList, ManifestStatus, PrimitiveLiteral, Schema, Snapshot,
};

/// Iceberg table provider for DataFusion
#[derive(Debug)]
pub struct IcebergTableProvider {
    /// The table location (URI)
    table_uri: String,
    /// The current schema of the table
    schema: Schema,
    /// The current snapshot of the table
    snapshot: Snapshot,
    /// Arrow schema for DataFusion
    arrow_schema: Arc<ArrowSchema>,
}

impl IcebergTableProvider {
    /// Create a new Iceberg table provider
    pub fn new(
        table_uri: impl ToString,
        schema: Schema,
        snapshot: Snapshot,
    ) -> DataFusionResult<Self> {
        let table_uri_str = table_uri.to_string();
        log::info!("[ICEBERG] Creating table provider for: {}", table_uri_str);

        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&schema).map_err(|e| {
            log::error!("[ICEBERG] Failed to convert schema to Arrow: {:?}", e);
            e
        })?);

        log::debug!(
            "[ICEBERG] Converted schema to Arrow with {} fields",
            arrow_schema.fields().len()
        );

        Ok(Self {
            table_uri: table_uri_str,
            schema,
            snapshot,
            arrow_schema,
        })
    }

    /// Get the table URI
    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Get the Iceberg schema
    pub fn iceberg_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get object store from DataFusion session
    fn get_object_store(
        &self,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn object_store::ObjectStore>> {
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        session
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
    }

    /// Load manifest list from snapshot
    async fn load_manifest_list(
        &self,
        object_store: &Arc<dyn object_store::ObjectStore>,
    ) -> DataFusionResult<ManifestList> {
        let manifest_list_str = self.snapshot.manifest_list();
        log::debug!("[ICEBERG] Manifest list path: {}", manifest_list_str);

        let manifest_list_path = if let Ok(url) = Url::parse(manifest_list_str) {
            log::debug!(
                "[ICEBERG] Parsed manifest list as URL, path: {}",
                url.path()
            );
            ObjectPath::from(url.path())
        } else {
            ObjectPath::from(manifest_list_str)
        };

        let manifest_list_data = object_store
            .get(&manifest_list_path)
            .await
            .map_err(|e| {
                log::error!("[ICEBERG] Failed to get manifest list: {:?}", e);
                datafusion::common::DataFusionError::External(Box::new(e))
            })?
            .bytes()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        log::debug!(
            "[ICEBERG] Read {} bytes from manifest list",
            manifest_list_data.len()
        );

        self.parse_manifest_list(&manifest_list_data)
    }

    /// Parse manifest list from Avro bytes
    fn parse_manifest_list(&self, data: &[u8]) -> DataFusionResult<ManifestList> {
        log::debug!("[ICEBERG] Parsing manifest list Avro data");
        let reader = AvroReader::new(data)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let mut manifest_files = Vec::new();
        for value in reader {
            let value =
                value.map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            log::trace!("[ICEBERG] Deserializing manifest file entry");
            let manifest_file: ManifestFileAvro = from_value(&value).map_err(|e| {
                log::error!("[ICEBERG] Failed to deserialize manifest file: {:?}", e);
                datafusion::common::DataFusionError::External(Box::new(e))
            })?;
            manifest_files.push(manifest_file.into());
        }

        Ok(ManifestList::new(manifest_files))
    }

    /// Load data files from manifests
    async fn load_data_files(
        &self,
        object_store: &Arc<dyn object_store::ObjectStore>,
        manifest_list: &ManifestList,
    ) -> DataFusionResult<Vec<DataFile>> {
        let mut data_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            // TODO: Support delete manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest_path_str = manifest_file.manifest_path.as_str();
            log::debug!("[ICEBERG] Loading manifest: {}", manifest_path_str);

            let manifest_path = if let Ok(url) = Url::parse(manifest_path_str) {
                log::debug!("[ICEBERG] Parsed manifest as URL, path: {}", url.path());
                ObjectPath::from(url.path())
            } else {
                ObjectPath::from(manifest_path_str)
            };

            let manifest_data = object_store
                .get(&manifest_path)
                .await
                .map_err(|e| {
                    log::error!("[ICEBERG] Failed to get manifest: {:?}", e);
                    datafusion::common::DataFusionError::External(Box::new(e))
                })?
                .bytes()
                .await
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

            log::debug!("[ICEBERG] Read {} bytes from manifest", manifest_data.len());

            let manifest_entries = self.parse_manifest(&manifest_data)?;

            // Get partition_spec_id from manifest file
            let partition_spec_id = manifest_file.partition_spec_id;

            for entry in manifest_entries {
                // Only include added and existing files, skip deleted files
                let status = match entry.status {
                    0 => ManifestStatus::Existing,
                    1 => ManifestStatus::Added,
                    2 => ManifestStatus::Deleted,
                    _ => ManifestStatus::Existing,
                };

                if matches!(status, ManifestStatus::Added | ManifestStatus::Existing) {
                    // Convert DataFileAvro to DataFile with schema and partition_spec_id
                    let data_file = entry
                        .data_file
                        .into_data_file(&self.schema, partition_spec_id);
                    data_files.push(data_file);
                }
            }
        }

        Ok(data_files)
    }

    /// Parse manifest from Avro bytes
    fn parse_manifest(&self, data: &[u8]) -> DataFusionResult<Vec<ManifestEntryAvro>> {
        log::debug!("[ICEBERG] Parsing manifest Avro data");
        let reader = AvroReader::new(data)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let mut entries = Vec::new();
        for value in reader {
            let value =
                value.map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            log::trace!("[ICEBERG] Deserializing data file entry");
            let mut entry: ManifestEntryAvro = from_value(&value).map_err(|e| {
                log::error!("[ICEBERG] Failed to deserialize data file entry: {:?}", e);
                datafusion::common::DataFusionError::External(Box::new(e))
            })?;

            // Extract map fields from raw Avro value
            if let apache_avro::types::Value::Record(fields) = &value {
                for (field_name, field_value) in fields {
                    if field_name == "data_file" {
                        if let apache_avro::types::Value::Record(data_file_fields) = field_value {
                            entry
                                .data_file
                                .extract_map_fields_from_avro(data_file_fields);
                        }
                    }
                }
            }

            entries.push(entry);
        }

        log::debug!("[ICEBERG] Parsed {} entries from manifest", entries.len());
        Ok(entries)
    }

    /// Create partitioned files for DataFusion from Iceberg data files
    fn create_partitioned_files(
        &self,
        data_files: Vec<DataFile>,
    ) -> DataFusionResult<Vec<PartitionedFile>> {
        let mut partitioned_files = Vec::new();

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let table_base_path = table_url.path();

        for data_file in data_files {
            let file_path_str = data_file.file_path();
            log::debug!("[ICEBERG] Processing data file: {}", file_path_str);

            let file_path = if let Ok(url) = Url::parse(file_path_str) {
                ObjectPath::from(url.path())
            } else {
                ObjectPath::from(format!(
                    "{}{}{}",
                    table_base_path,
                    object_store::path::DELIMITER,
                    file_path_str
                ))
            };

            log::debug!("[ICEBERG] Final ObjectPath: {}", file_path);

            let object_meta = ObjectMeta {
                location: file_path,
                last_modified: chrono::Utc::now(),
                size: data_file.file_size_in_bytes(),
                e_tag: None,
                version: None,
            };

            // Convert partition values to ScalarValues
            let partition_values = data_file
                .partition()
                .iter()
                .map(|literal_opt| match literal_opt {
                    Some(literal) => self.literal_to_scalar_value(literal),
                    None => ScalarValue::Null,
                })
                .collect();

            let partitioned_file = PartitionedFile {
                object_meta,
                partition_values,
                range: None,
                statistics: Some(Arc::new(self.create_file_statistics(&data_file))),
                extensions: None,
                metadata_size_hint: None,
            };

            partitioned_files.push(partitioned_file);
        }

        Ok(partitioned_files)
    }

    /// Create file groups from partitioned files
    fn create_file_groups(&self, partitioned_files: Vec<PartitionedFile>) -> Vec<FileGroup> {
        // Group files by partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        for file in partitioned_files {
            file_groups
                .entry(file.partition_values.clone())
                .or_default()
                .push(file);
        }

        file_groups.into_values().map(FileGroup::from).collect()
    }

    /// Convert Iceberg Literal to DataFusion ScalarValue
    fn literal_to_scalar_value(&self, literal: &Literal) -> ScalarValue {
        match literal {
            Literal::Primitive(primitive) => match primitive {
                PrimitiveLiteral::Boolean(v) => ScalarValue::Boolean(Some(*v)),
                PrimitiveLiteral::Int(v) => ScalarValue::Int32(Some(*v)),
                PrimitiveLiteral::Long(v) => ScalarValue::Int64(Some(*v)),
                PrimitiveLiteral::Float(v) => ScalarValue::Float32(Some(v.into_inner())),
                PrimitiveLiteral::Double(v) => ScalarValue::Float64(Some(v.into_inner())),
                PrimitiveLiteral::String(v) => ScalarValue::Utf8(Some(v.clone())),
                PrimitiveLiteral::Binary(v) => ScalarValue::Binary(Some(v.clone())),
                PrimitiveLiteral::Int128(v) => ScalarValue::Decimal128(Some(*v), 38, 0),
                PrimitiveLiteral::UInt128(v) => {
                    if *v <= i128::MAX as u128 {
                        ScalarValue::Decimal128(Some(*v as i128), 38, 0)
                    } else {
                        ScalarValue::Utf8(Some(v.to_string()))
                    }
                }
            },
            Literal::Struct(fields) => {
                let json_repr = serde_json::to_string(fields).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
            Literal::List(items) => {
                let json_repr = serde_json::to_string(items).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
            Literal::Map(pairs) => {
                let json_repr = serde_json::to_string(pairs).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
        }
    }

    /// Create file statistics from Iceberg data file metadata
    fn create_file_statistics(&self, data_file: &DataFile) -> Statistics {
        let num_rows = Precision::Exact(data_file.record_count() as usize);
        let total_byte_size = Precision::Exact(data_file.file_size_in_bytes() as usize);

        // Create column statistics from Iceberg metadata
        let column_statistics = self
            .arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _field)| {
                let field_id = self
                    .schema
                    .fields()
                    .get(i)
                    .map(|f| f.id)
                    .unwrap_or(i as i32 + 1);

                let null_count = data_file
                    .null_value_counts()
                    .get(&field_id)
                    .map(|&count| Precision::Exact(count as usize))
                    .unwrap_or(Precision::Absent);

                let distinct_count = Precision::Absent;

                let min_value = data_file
                    .lower_bounds()
                    .get(&field_id)
                    .map(|literal| self.literal_to_scalar_value(literal))
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                let max_value = data_file
                    .upper_bounds()
                    .get(&field_id)
                    .map(|literal| self.literal_to_scalar_value(literal))
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                ColumnStatistics {
                    null_count,
                    max_value,
                    min_value,
                    distinct_count,
                    sum_value: Precision::Absent,
                }
            })
            .collect();

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        }
    }
}

/// Avro representation of ManifestFile for deserialization
#[derive(Debug, Serialize, Deserialize)]
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
    #[serde(rename = "added_files_count")]
    added_files_count: i32,
    #[serde(rename = "existing_files_count")]
    existing_files_count: i32,
    #[serde(rename = "deleted_files_count")]
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

/// Parse Avro map format (array of {key, value} objects) to HashMap for i64 values
fn parse_i64_map_from_avro(values: &Option<apache_avro::types::Value>) -> HashMap<i32, i64> {
    use apache_avro::types::Value;

    let mut map = HashMap::new();

    let vec_opt = if let Some(Value::Union(_, boxed)) = values {
        if let Value::Array(vec) = boxed.as_ref() {
            Some(vec)
        } else {
            None
        }
    } else if let Some(Value::Array(vec)) = values {
        Some(vec)
    } else {
        None
    };

    if let Some(vec) = vec_opt {
        for item in vec {
            if let Value::Record(fields) = item {
                let mut key_opt = None;
                let mut value_opt = None;

                for (field_name, field_value) in fields {
                    match field_name.as_str() {
                        "key" => {
                            if let Value::Int(k) = field_value {
                                key_opt = Some(*k);
                            }
                        }
                        "value" => {
                            if let Value::Long(v) = field_value {
                                value_opt = Some(*v);
                            }
                        }
                        _ => {}
                    }
                }

                if let (Some(key), Some(value)) = (key_opt, value_opt) {
                    map.insert(key, value);
                }
            }
        }
    }

    map
}

/// Parse Avro map format for byte arrays from Avro Values
fn parse_bytes_map_from_avro(
    values: &Option<apache_avro::types::Value>,
) -> Option<HashMap<i32, Vec<u8>>> {
    use apache_avro::types::Value;

    if let Some(Value::Union(_, boxed)) = values {
        if let Value::Array(vec) = boxed.as_ref() {
            let mut map = HashMap::new();
            for item in vec {
                if let Value::Record(fields) = item {
                    let mut key_opt = None;
                    let mut value_opt = None;

                    for (field_name, field_value) in fields {
                        match field_name.as_str() {
                            "key" => {
                                if let Value::Int(k) = field_value {
                                    key_opt = Some(*k);
                                }
                            }
                            "value" => {
                                if let Value::Bytes(b) = field_value {
                                    value_opt = Some(b.clone());
                                }
                            }
                            _ => {}
                        }
                    }

                    if let (Some(key), Some(value)) = (key_opt, value_opt) {
                        map.insert(key, value);
                    }
                }
            }
            return Some(map);
        }
    } else if let Some(Value::Array(vec)) = values {
        let mut map = HashMap::new();
        for item in vec {
            if let Value::Record(fields) = item {
                let mut key_opt = None;
                let mut value_opt = None;

                for (field_name, field_value) in fields {
                    match field_name.as_str() {
                        "key" => {
                            if let Value::Int(k) = field_value {
                                key_opt = Some(*k);
                            }
                        }
                        "value" => {
                            if let Value::Bytes(b) = field_value {
                                value_opt = Some(b.clone());
                            }
                        }
                        _ => {}
                    }
                }

                if let (Some(key), Some(value)) = (key_opt, value_opt) {
                    map.insert(key, value);
                }
            }
        }
        return Some(map);
    }

    None
}

/// Avro representation of ManifestEntry for deserialization
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
    column_sizes: Option<apache_avro::types::Value>,
    #[serde(skip)]
    value_counts: Option<apache_avro::types::Value>,
    #[serde(skip)]
    null_value_counts: Option<apache_avro::types::Value>,
    #[serde(skip)]
    nan_value_counts: Option<apache_avro::types::Value>,
    #[serde(skip)]
    lower_bounds: Option<apache_avro::types::Value>,
    #[serde(skip)]
    upper_bounds: Option<apache_avro::types::Value>,
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
    /// Extract map fields from raw Avro record fields
    fn extract_map_fields_from_avro(&mut self, fields: &[(String, apache_avro::types::Value)]) {
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

    /// Convert DataFileAvro to DataFile with schema context for proper bound parsing
    fn into_data_file(self, schema: &Schema, partition_spec_id: i32) -> DataFile {
        let content = match self.content {
            0 => DataContentType::Data,
            1 => DataContentType::PositionDeletes,
            2 => DataContentType::EqualityDeletes,
            _ => DataContentType::Data,
        };

        let file_format = match self.file_format.to_uppercase().as_str() {
            "PARQUET" => crate::spec::DataFileFormat::Parquet,
            "AVRO" => crate::spec::DataFileFormat::Avro,
            "ORC" => crate::spec::DataFileFormat::Orc,
            _ => crate::spec::DataFileFormat::Parquet, // Default
        };

        // Parse partition values from JSON
        let partition = parse_partition_values(Some(&self.partition));

        // Parse Avro map arrays (array of {key, value} records)
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

        // Parse bounds from binary data using schema for proper type conversion
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

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::info!("[ICEBERG] Starting scan for table: {}", self.table_uri);

        let object_store = self.get_object_store(session)?;
        log::debug!("[ICEBERG] Got object store");

        log::info!(
            "[ICEBERG] Loading manifest list from: {}",
            self.snapshot.manifest_list()
        );
        let manifest_list = self.load_manifest_list(&object_store).await?;
        log::info!(
            "[ICEBERG] Loaded {} manifest files",
            manifest_list.entries().len()
        );

        log::info!("[ICEBERG] Loading data files from manifests...");
        let data_files = self.load_data_files(&object_store, &manifest_list).await?;
        log::info!("[ICEBERG] Loaded {} data files", data_files.len());

        log::info!("[ICEBERG] Creating partitioned files...");
        let partitioned_files = self.create_partitioned_files(data_files)?;
        log::info!(
            "[ICEBERG] Created {} partitioned files",
            partitioned_files.len()
        );

        // Step 4: Create file groups
        let file_groups = self.create_file_groups(partitioned_files);

        // Step 5: Create file scan configuration
        let file_schema = self.arrow_schema.clone();
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let base_url = format!("{}://{}", table_url.scheme(), table_url.authority());
        let base_url_parsed = Url::parse(&base_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let object_store_url = ObjectStoreUrl::parse(base_url_parsed)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let parquet_source = Arc::new(ParquetSource::new(parquet_options));

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, file_schema, parquet_source)
                .with_file_groups(if file_groups.is_empty() {
                    vec![FileGroup::from(vec![])]
                } else {
                    file_groups
                })
                .with_statistics(Statistics::new_unknown(&self.arrow_schema))
                .with_projection(projection.cloned())
                .with_limit(limit)
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

/// Parse partition values from JSON
fn parse_partition_values(partition_json: Option<&serde_json::Value>) -> Vec<Option<Literal>> {
    match partition_json {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| match value {
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
        Some(serde_json::Value::Object(_)) => {
            vec![None]
        }
        _ => Vec::new(),
    }
}

/// Parse bounds from binary data using schema field types
fn parse_bounds_from_binary(
    bounds_data: Option<&HashMap<i32, Vec<u8>>>,
    schema: &Schema,
) -> HashMap<i32, Literal> {
    use crate::spec::Type;

    let mut bounds = HashMap::new();

    if let Some(data) = bounds_data {
        for (field_id, binary_data) in data {
            // Find the field in schema to get its type
            if let Some(field) = schema.field_by_id(*field_id) {
                let field_type = field.field_type.as_ref();

                // Parse based on primitive type
                let literal = match field_type {
                    Type::Primitive(prim_type) => {
                        parse_primitive_bound(binary_data, prim_type).ok()
                    }
                    _ => None,
                };

                if let Some(lit) = literal {
                    bounds.insert(*field_id, lit);
                }
            } else {
                // Fallback: if field not found, try to parse as string or binary
                if let Ok(string_value) = String::from_utf8(binary_data.clone()) {
                    bounds.insert(
                        *field_id,
                        Literal::Primitive(PrimitiveLiteral::String(string_value)),
                    );
                } else {
                    bounds.insert(
                        *field_id,
                        Literal::Primitive(PrimitiveLiteral::Binary(binary_data.clone())),
                    );
                }
            }
        }
    }

    bounds
}

/// Parse a primitive bound value from binary data based on its type
/// Reference: https://iceberg.apache.org/spec/#binary-single-value-serialization
fn parse_primitive_bound(
    bytes: &[u8],
    prim_type: &crate::spec::PrimitiveType,
) -> Result<Literal, String> {
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
                // Handle schema evolution case
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
                // Handle schema evolution case
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

    Ok(Literal::Primitive(literal))
}
