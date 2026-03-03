// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Cursor};
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray, StructArray};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Schema as ArrowSchema,
};
use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::kernel::arrow::engine_ext::{parse_partition_values_array, stats_schema};
use crate::kernel::checkpoints::{latest_replayable_version, load_replayed_table_state};
use crate::kernel::snapshot::iterators::LogicalFileView;
pub use crate::kernel::snapshot::log_data::LogDataHandler;
use crate::kernel::{DeltaResult, DeltaTableConfig, DeltaTableError, PredicateRef, SchemaRef};
use crate::spec::{
    Add, ColumnMappingMode, CommitInfo, Metadata, Protocol, Remove, StorageType, TableProperties,
    Transaction,
};
use crate::storage::LogStore;

pub mod iterators;
pub mod log_data;

pub(crate) type SendableRBStream =
    Pin<Box<dyn futures::Stream<Item = DeltaResult<RecordBatch>> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotTableConfiguration {
    metadata: Metadata,
    protocol: Protocol,
    schema: SchemaRef,
    table_properties: TableProperties,
}

impl SnapshotTableConfiguration {
    fn new(metadata: Metadata, protocol: Protocol, schema: SchemaRef) -> Self {
        let table_properties = TableProperties::from(metadata.configuration().iter());
        Self {
            metadata,
            protocol,
            schema,
            table_properties,
        }
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.table_properties
            .column_mapping_mode
            .unwrap_or(ColumnMappingMode::None)
    }
}

/// A snapshot of a Delta table.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    version: i64,
    config: DeltaTableConfig,
    table_url: Url,
    table_configuration: SnapshotTableConfiguration,
    active_adds: Vec<Add>,
    active_removes: Vec<Remove>,
    app_txns: HashMap<String, Transaction>,
    commit_timestamps: BTreeMap<i64, i64>,
}

impl Snapshot {
    /// Create a new [`Snapshot`] instance.
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let target_version = match version {
            Some(v) => v,
            None => match latest_replayable_version(log_store).await {
                Ok(v) => v,
                Err(crate::error::DeltaError::MissingVersion) => {
                    return Err(DeltaTableError::invalid_table_location(
                        "No commit files found in _delta_log",
                    ))
                }
                Err(err) => return Err(err),
            },
        };
        let replayed = load_replayed_table_state(target_version, log_store).await?;
        let schema = Arc::new(replayed.metadata.parse_schema_arrow()?);
        let table_configuration =
            SnapshotTableConfiguration::new(replayed.metadata, replayed.protocol, schema);

        Ok(Self {
            version: replayed.version,
            config,
            table_url: log_store.config().location.clone(),
            table_configuration,
            active_adds: replayed.adds,
            active_removes: replayed.removes,
            app_txns: replayed.txns,
            commit_timestamps: replayed.commit_timestamps,
        })
    }

    /// Update the snapshot to the given version.
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        let target_version = match target_version {
            Some(v) => i64::try_from(v)
                .map_err(|_| DeltaTableError::generic("target version overflows i64"))?,
            None => log_store.get_latest_version(self.version()).await?,
        };

        if target_version == self.version() {
            return Ok(());
        }
        if target_version < self.version() {
            return Err(DeltaTableError::generic("Cannot downgrade snapshot"));
        }

        *self = Self::try_new(log_store, self.config.clone(), Some(target_version)).await?;
        Ok(())
    }

    /// Get the table version of the snapshot.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Get the table schema of the snapshot.
    pub fn schema(&self) -> &ArrowSchema {
        self.table_configuration.schema().as_ref()
    }

    /// Get the table metadata of the snapshot.
    pub fn metadata(&self) -> &Metadata {
        self.table_configuration.metadata()
    }

    /// Get the table protocol of the snapshot.
    pub fn protocol(&self) -> &Protocol {
        self.table_configuration.protocol()
    }

    /// Get the table config which is loaded with of the snapshot.
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    /// Get the table root of the snapshot.
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        Ok(Path::from_url_path(self.table_url.path())?)
    }

    /// Well known properties of the table.
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration.table_properties()
    }

    pub fn table_configuration(&self) -> &SnapshotTableConfiguration {
        &self.table_configuration
    }

    fn build_files_batch_from_adds(&self, adds: &[Add]) -> DeltaResult<RecordBatch> {
        let rows = adds
            .iter()
            .cloned()
            .map(SnapshotAddRow::from)
            .collect::<Vec<_>>();
        let raw = encode_snapshot_add_rows(&rows)?;
        parse_scan_row_columns(raw, self)
    }

    fn build_active_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(&self.active_adds)
    }

    fn build_empty_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(&[])
    }

    /// Get the active files for the current snapshot.
    #[expect(dead_code)]
    pub fn files(
        &self,
        _log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        if predicate.is_some() {
            return Box::pin(futures::stream::once(async {
                Err(DeltaTableError::generic(
                    "Snapshot::files predicate pushdown is not supported in native replay mode",
                ))
            }));
        }
        match self.build_active_files_batch() {
            Ok(batch) => Box::pin(futures::stream::once(async { Ok(batch) })),
            Err(err) => Box::pin(futures::stream::once(async { Err(err) })),
        }
    }

    /// Get the commit infos in the snapshot.
    #[expect(dead_code)]
    pub(crate) async fn commit_infos(
        &self,
        log_store: &dyn LogStore,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let store = log_store.root_object_store(None);
        let log_root = self.table_root_path()?.child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );
        let dummy_url = url::Url::parse("memory:///")
            .map_err(|e| DeltaTableError::generic(format!("Failed to parse dummy URL: {e}")))?;
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            let location = dummy_url
                .join(meta.location.as_ref())
                .map_err(|e| DeltaTableError::generic(format!("Failed to join URL path: {e}")))?;
            if parse_commit_version_from_path(location.path()).is_some() {
                commit_files.push(meta);
            }
        }
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        Ok(futures::stream::iter(commit_files)
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;
                    let reader = BufReader::new(Cursor::new(commit_log_bytes));
                    for line in reader.lines() {
                        let action: crate::spec::Action = serde_json::from_str(line?.as_str())?;
                        if let crate::spec::Action::CommitInfo(commit_info) = action {
                            return Ok::<_, DeltaTableError>(Some(commit_info));
                        }
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed())
    }

    pub(crate) fn tombstones(
        &self,
        _log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<Remove>> {
        futures::stream::iter(
            self.active_removes
                .clone()
                .into_iter()
                .map(Ok::<_, DeltaTableError>),
        )
        .boxed()
    }

    async fn application_transaction_version(
        &self,
        _log_store: &dyn LogStore,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        Ok(self.app_txns.get(&app_id).map(|txn| txn.version))
    }
}

fn parse_commit_version_from_path(path: &str) -> Option<i64> {
    let filename = path.rsplit('/').next()?;
    if filename.len() != 25 || !filename.ends_with(".json") {
        return None;
    }
    let prefix = filename.get(0..20)?;
    if !prefix.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    prefix.parse::<i64>().ok()
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    pub(crate) files: RecordBatch,
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance.
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;
        let files = if config.require_files {
            snapshot.build_active_files_batch()?
        } else {
            snapshot.build_empty_files_batch()?
        };
        Ok(Self { snapshot, files })
    }

    /// Update the snapshot to the given version.
    pub(crate) async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        self.snapshot.update(log_store, target_version).await?;
        self.files = if self.snapshot.load_config().require_files {
            self.snapshot.build_active_files_batch()?
        } else {
            self.snapshot.build_empty_files_batch()?
        };
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        self.snapshot.commit_timestamps.get(&version).copied()
    }

    pub fn schema(&self) -> &ArrowSchema {
        self.snapshot.schema()
    }

    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    pub fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    pub fn config(&self) -> &TableProperties {
        self.table_properties()
    }

    pub fn table_configuration(&self) -> &SnapshotTableConfiguration {
        self.snapshot.table_configuration()
    }

    pub fn log_data(&self) -> LogDataHandler<'_> {
        LogDataHandler::new(&self.files, self.snapshot.table_configuration())
    }

    pub fn files(
        &self,
        _log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        if predicate.is_some() {
            return Box::pin(futures::stream::once(async {
                Err(DeltaTableError::generic(
                    "EagerSnapshot::files predicate pushdown is not supported in native replay mode",
                ))
            }));
        }
        let batch = self.files.clone();
        let iter = (0..batch.num_rows()).map(move |i| Ok(LogicalFileView::new(batch.clone(), i)));
        futures::stream::iter(iter).boxed()
    }

    pub async fn transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: impl ToString,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot
            .application_transaction_version(log_store, app_id.to_string())
            .await
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotDeletionVectorRow {
    storage_type: String,
    path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotAddRow {
    #[serde(with = "serde_path_compat")]
    path: String,
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<SnapshotDeletionVectorRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_row_commit_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    clustering_provider: Option<String>,
}

impl From<Add> for SnapshotAddRow {
    fn from(value: Add) -> Self {
        Self {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(|dv| SnapshotDeletionVectorRow {
                storage_type: dv.storage_type.as_ref().to_string(),
                path_or_inline_dv: dv.path_or_inline_dv,
                offset: dv.offset,
                size_in_bytes: dv.size_in_bytes,
                cardinality: dv.cardinality,
            }),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

fn snapshot_add_probe_row() -> SnapshotAddRow {
    SnapshotAddRow {
        path: "_probe.parquet".to_string(),
        partition_values: HashMap::from([("p".to_string(), Some("x".to_string()))]),
        size: 0,
        modification_time: 0,
        data_change: true,
        stats: Some("{}".to_string()),
        tags: Some(HashMap::from([("t".to_string(), Some("x".to_string()))])),
        deletion_vector: Some(SnapshotDeletionVectorRow {
            storage_type: StorageType::UuidRelativePath.as_ref().to_string(),
            path_or_inline_dv: "dv.bin".to_string(),
            offset: Some(0),
            size_in_bytes: 1,
            cardinality: 1,
        }),
        base_row_id: Some(0),
        default_row_commit_version: Some(0),
        clustering_provider: Some("none".to_string()),
    }
}

fn snapshot_add_tracing_options() -> serde_arrow::schema::TracingOptions {
    fn map_utf8_utf8(field_name: &str, nullable: bool) -> Field {
        let entry_struct = ArrowDataType::Struct(
            vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
                Arc::new(Field::new("value", ArrowDataType::Utf8, true)),
            ]
            .into(),
        );
        Field::new(
            field_name,
            ArrowDataType::Map(
                Arc::new(Field::new("key_value", entry_struct, false)),
                false,
            ),
            nullable,
        )
    }

    serde_arrow::schema::TracingOptions::default()
        .map_as_struct(false)
        .allow_null_fields(true)
        .strings_as_large_utf8(false)
        .sequence_as_large_list(false)
        .overwrite("partitionValues", map_utf8_utf8("partitionValues", false))
        .expect("snapshot tracing overwrite for partitionValues should be valid")
        .overwrite("tags", map_utf8_utf8("tags", true))
        .expect("snapshot tracing overwrite for tags should be valid")
}

fn encode_snapshot_add_rows(rows: &[SnapshotAddRow]) -> DeltaResult<RecordBatch> {
    use serde_arrow::schema::SchemaLike;

    let mut samples = rows.to_vec();
    samples.push(snapshot_add_probe_row());
    let fields = Vec::<FieldRef>::from_samples(&samples, snapshot_add_tracing_options())
        .map_err(DeltaTableError::generic_err)?;
    let owned_rows = rows.to_vec();
    serde_arrow::to_record_batch(&fields, &owned_rows).map_err(DeltaTableError::generic_err)
}

fn build_partition_schema(
    schema: &ArrowSchema,
    partition_columns: &[String],
) -> DeltaResult<Option<ArrowSchema>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    let fields = partition_columns
        .iter()
        .map(|col| {
            schema
                .field_with_name(col)
                .map(|f| f.clone())
                .map_err(|_| DeltaTableError::missing_column(col))
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(Some(ArrowSchema::new(fields)))
}

fn build_stats_source_schema(snapshot: &Snapshot) -> DeltaResult<ArrowSchema> {
    use crate::schema::make_physical_arrow_schema;
    let partition_columns = snapshot.metadata().partition_columns();
    let mode = snapshot.table_configuration.column_mapping_mode();
    let non_partition_fields: Vec<Field> = snapshot
        .schema()
        .fields()
        .iter()
        .filter(|field| !partition_columns.contains(field.name()))
        .map(|f| f.as_ref().clone())
        .collect();
    let logical_non_partition = ArrowSchema::new(non_partition_fields);
    Ok(make_physical_arrow_schema(&logical_non_partition, mode))
}

fn parse_scan_row_columns(raw: RecordBatch, snapshot: &Snapshot) -> DeltaResult<RecordBatch> {
    let mut fields = raw.schema().fields().to_vec();
    let mut columns = raw.columns().to_vec();
    let mode = snapshot.table_configuration.column_mapping_mode();

    if let Some((stats_idx, _)) = raw.schema_ref().column_with_name("stats") {
        let stats_source_arrow = build_stats_source_schema(snapshot)?;
        let stats_source_kernel = crate::schema::logical_arrow_to_kernel(&stats_source_arrow)?;
        let stats_schema = Arc::new(stats_schema(
            &stats_source_kernel,
            snapshot.table_properties(),
        )?);
        let arrow_stats_schema = Arc::new(ArrowSchema::try_from(stats_schema.as_ref())?);
        let stats_batch = raw.project(&[stats_idx])?;
        let stats_json = stats_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DeltaTableError::schema("expected Utf8 stats column when parsing stats".to_string())
            })?;
        let mut json_lines = String::new();
        for value in stats_json.iter() {
            if let Some(value) = value {
                json_lines.push_str(value);
            } else {
                json_lines.push_str("{}");
            }
            json_lines.push('\n');
        }
        let mut reader = JsonReaderBuilder::new(Arc::clone(&arrow_stats_schema))
            .with_batch_size(stats_batch.num_rows().max(1))
            .build(Cursor::new(json_lines))
            .map_err(DeltaTableError::generic_err)?;
        let parsed_batch = match reader.next() {
            Some(batch) => batch.map_err(DeltaTableError::generic_err)?,
            None => RecordBatch::new_empty(arrow_stats_schema),
        };
        let stats_array: Arc<StructArray> = Arc::new(parsed_batch.into());
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            stats_array.data_type().to_owned(),
            true,
        )));
        columns.push(stats_array);
    }

    if let Some(partition_schema_arrow) =
        build_partition_schema(snapshot.schema(), snapshot.metadata().partition_columns())?
    {
        let partition_schema = crate::schema::logical_arrow_to_kernel(&partition_schema_arrow)?;
        let partition_array =
            parse_partition_values_array(&raw, &partition_schema, "partitionValues", mode)?;
        fields.push(Arc::new(Field::new(
            "partitionValues_parsed",
            partition_array.data_type().to_owned(),
            false,
        )));
        columns.push(Arc::new(partition_array));
    }

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

mod serde_path_compat {
    use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    const INVALID: &AsciiSet = &CONTROLS
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
        .add(b'<')
        .add(b'#')
        .add(b'|')
        .add(b'\r')
        .add(b'\n')
        .add(b'*')
        .add(b'?');

    pub fn serialize<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = percent_encode(value.as_bytes(), INVALID).to_string();
        String::serialize(&encoded, serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        percent_decode_str(&s)
            .decode_utf8()
            .map(|v| v.to_string())
            .map_err(serde::de::Error::custom)
    }
}
