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
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{
    Field, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use once_cell::sync::OnceCell;
use url::Url;

use crate::kernel::checkpoints::{
    latest_replayable_version, load_replayed_table_header, load_replayed_table_state,
    ReplayedTableHeader, ReplayedTableState,
};
pub use crate::kernel::snapshot::stats::SnapshotPruningStats;
use crate::kernel::{DeltaTableConfig, SchemaRef};
use crate::schema::{arrow_field_physical_name, arrow_schema_reorder_partitions};
use crate::spec::fields::{
    FIELD_NAME_MODIFICATION_TIME, FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_PATH,
    FIELD_NAME_SIZE, FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
    STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{
    Add, ColumnMappingMode, ColumnMetadataKey, DeltaError as DeltaTableError, DeltaResult,
    Metadata, Protocol, Remove, TableFeature, TableProperties, Transaction, VersionChecksum,
};
use crate::storage::LogStore;

mod materialize;
mod stats;

pub struct DeltaSnapshot {
    version: i64,
    table_url: Url,
    config: DeltaTableConfig,
    protocol: Protocol,
    metadata: Metadata,
    table_properties: TableProperties,
    arrow_schema: SchemaRef,
    adds: Arc<Vec<Add>>,
    removes: Arc<Vec<Remove>>,
    app_txns: Arc<HashMap<String, Transaction>>,
    commit_timestamps: Arc<BTreeMap<i64, i64>>,
    files_batch: OnceCell<RecordBatch>,
}

impl std::fmt::Debug for DeltaSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaSnapshot")
            .field("version", &self.version)
            .field("table_url", &self.table_url)
            .field("require_files", &self.config.require_files)
            .field("adds_len", &self.adds.len())
            .field("removes_len", &self.removes.len())
            .finish()
    }
}

impl Clone for DeltaSnapshot {
    fn clone(&self) -> Self {
        let files_batch = OnceCell::new();
        if let Some(batch) = self.files_batch.get() {
            let _ = files_batch.set(batch.clone());
        }
        Self {
            version: self.version,
            table_url: self.table_url.clone(),
            config: self.config.clone(),
            protocol: self.protocol.clone(),
            metadata: self.metadata.clone(),
            table_properties: self.table_properties.clone(),
            arrow_schema: Arc::clone(&self.arrow_schema),
            adds: Arc::clone(&self.adds),
            removes: Arc::clone(&self.removes),
            app_txns: Arc::clone(&self.app_txns),
            commit_timestamps: Arc::clone(&self.commit_timestamps),
            files_batch,
        }
    }
}

impl DeltaSnapshot {
    pub(crate) async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
        replay_hint: Option<&ReplayedTableHeader>,
    ) -> DeltaResult<Self> {
        let target_version = match version {
            Some(v) => v,
            None => match latest_replayable_version(log_store).await {
                Ok(v) => v,
                Err(crate::spec::DeltaError::MissingVersion) => {
                    return Err(DeltaTableError::invalid_table_location(
                        "No commit files found in _delta_log",
                    ))
                }
                Err(err) => return Err(err),
            },
        };

        if !config.require_files {
            match load_replayed_table_header(target_version, log_store, replay_hint).await {
                Ok(Some(replayed)) => {
                    return Self::from_replayed_header(log_store, config, replayed)
                }
                Ok(None) => {}
                Err(err) => {
                    debug!(
                        "Failed to load table header fast-path for version {}: {}; falling back to full replay",
                        target_version, err
                    );
                }
            }
        }

        let replayed = load_replayed_table_state(target_version, log_store).await?;
        Self::from_replayed_state(log_store, config, replayed)
    }

    fn from_replayed_state(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        replayed: ReplayedTableState,
    ) -> DeltaResult<Self> {
        Self::from_replayed_parts(
            log_store,
            config,
            replayed.version,
            replayed.protocol,
            replayed.metadata,
            replayed.adds,
            replayed.removes,
            replayed.txns,
            replayed.commit_timestamps,
        )
    }

    fn from_replayed_header(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        replayed: ReplayedTableHeader,
    ) -> DeltaResult<Self> {
        let arrow_schema = Arc::new(replayed.metadata.parse_schema_arrow()?);
        let table_properties = TableProperties::from(replayed.metadata.configuration().iter());

        Ok(Self {
            version: replayed.version,
            table_url: log_store.config().location.clone(),
            config,
            protocol: replayed.protocol,
            metadata: replayed.metadata,
            table_properties,
            arrow_schema,
            adds: Arc::new(Vec::new()),
            removes: Arc::new(Vec::new()),
            app_txns: replayed.txns,
            commit_timestamps: replayed.commit_timestamps,
            files_batch: OnceCell::new(),
        })
    }

    #[expect(clippy::too_many_arguments)]
    fn from_replayed_parts(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: i64,
        protocol: Protocol,
        metadata: Metadata,
        adds: Vec<Add>,
        removes: Vec<Remove>,
        txns: HashMap<String, Transaction>,
        commit_timestamps: BTreeMap<i64, i64>,
    ) -> DeltaResult<Self> {
        let arrow_schema = Arc::new(metadata.parse_schema_arrow()?);
        let table_properties = TableProperties::from(metadata.configuration().iter());

        Ok(Self {
            version,
            table_url: log_store.config().location.clone(),
            config,
            protocol,
            metadata,
            table_properties,
            arrow_schema,
            adds: Arc::new(adds),
            removes: Arc::new(removes),
            app_txns: Arc::new(txns),
            commit_timestamps: Arc::new(commit_timestamps),
            files_batch: OnceCell::new(),
        })
    }

    fn replay_hint(&self) -> ReplayedTableHeader {
        ReplayedTableHeader {
            version: self.version,
            protocol: self.protocol.clone(),
            metadata: self.metadata.clone(),
            txns: Arc::clone(&self.app_txns),
            commit_timestamps: Arc::clone(&self.commit_timestamps),
        }
    }

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

        let replay_hint = (!self.config.require_files).then(|| self.replay_hint());
        *self = Self::try_new(
            log_store,
            self.config.clone(),
            Some(target_version),
            replay_hint.as_ref(),
        )
        .await?;
        Ok(())
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        self.commit_timestamps.get(&version).copied()
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn schema(&self) -> &ArrowSchema {
        self.arrow_schema.as_ref()
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    pub fn table_properties(&self) -> &TableProperties {
        &self.table_properties
    }

    pub fn config(&self) -> &TableProperties {
        self.table_properties()
    }

    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.table_properties
            .column_mapping_mode
            .unwrap_or(ColumnMappingMode::None)
    }

    pub fn effective_column_mapping_mode(&self) -> ColumnMappingMode {
        let explicit = self.column_mapping_mode();
        if matches!(explicit, ColumnMappingMode::None) {
            let has_annotations = self.schema().fields().iter().any(|field| {
                field
                    .metadata()
                    .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                    && field
                        .metadata()
                        .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
            });
            if has_annotations {
                return ColumnMappingMode::Name;
            }
        }
        explicit
    }

    pub fn logical_arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        self.input_schema()
    }

    pub fn arrow_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_reorder_partitions(self.schema(), self.metadata().partition_columns(), true)
    }

    pub fn input_schema(&self) -> DeltaResult<ArrowSchemaRef> {
        arrow_schema_reorder_partitions(self.schema(), self.metadata().partition_columns(), false)
    }

    pub fn adds(&self) -> &[Add] {
        self.adds.as_ref()
    }

    pub fn removes(&self) -> &[Remove] {
        self.removes.as_ref()
    }

    fn has_unknown_table_features(&self) -> bool {
        self.protocol()
            .reader_features()
            .into_iter()
            .flatten()
            .chain(self.protocol().writer_features().into_iter().flatten())
            .any(|feature| matches!(feature, TableFeature::Unknown))
    }

    fn has_deletion_vectors(&self) -> bool {
        self.adds().iter().any(|add| add.deletion_vector.is_some())
            || self
                .removes()
                .iter()
                .any(|remove| remove.deletion_vector.is_some())
    }

    pub fn build_version_checksum(
        &self,
        txn_id: Option<String>,
        in_commit_timestamp_opt: Option<i64>,
    ) -> DeltaResult<Option<VersionChecksum>> {
        // TODO: Remove these coarse skips once replay retains the latest
        // DomainMetadata actions and reconciles files with deletion-vector identity.
        if self.has_unknown_table_features() {
            debug!(
                "Skipping version checksum for version {} because the protocol includes unsupported features",
                self.version()
            );
            return Ok(None);
        }
        if self.has_deletion_vectors() {
            debug!(
                "Skipping version checksum for version {} because the snapshot includes deletion vectors",
                self.version()
            );
            return Ok(None);
        }

        let mut num_files: i64 = 0;
        let mut table_size_bytes: i64 = 0;

        for add in self.adds() {
            num_files = num_files
                .checked_add(1)
                .ok_or_else(|| DeltaTableError::generic("Version checksum file count overflow"))?;
            table_size_bytes = table_size_bytes
                .checked_add(add.size)
                .ok_or_else(|| DeltaTableError::generic("Version checksum table size overflow"))?;
        }

        let mut set_transactions = self.app_txns.values().cloned().collect::<Vec<_>>();
        set_transactions.sort_by(|left, right| {
            left.app_id
                .cmp(&right.app_id)
                .then(left.version.cmp(&right.version))
        });

        Ok(Some(VersionChecksum {
            txn_id,
            table_size_bytes,
            num_files,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt,
            set_transactions: (!set_transactions.is_empty()).then_some(set_transactions),
            // TODO(protocol-hardening): Populate from reconciled snapshot state once replay keeps
            // the latest DomainMetadata actions alongside metadata/protocol/txns.
            domain_metadata: None,
            metadata: self.metadata.clone(),
            protocol: self.protocol.clone(),
            // TODO(protocol-hardening): Populate optional protocol fields when we can do so
            // deterministically without synthesizing partial state.
            file_size_histogram: None,
            all_files: None,
        }))
    }

    pub fn physical_partition_columns(&self) -> Vec<(String, String)> {
        let mode = self.effective_column_mapping_mode();
        self.metadata()
            .partition_columns()
            .iter()
            .map(|logical| {
                let physical = self
                    .schema()
                    .field_with_name(logical)
                    .map(|field| arrow_field_physical_name(field, mode).to_string())
                    .unwrap_or_else(|_| logical.clone());
                (logical.clone(), physical)
            })
            .collect()
    }

    pub fn files_batch(&self) -> DeltaResult<&RecordBatch> {
        self.files_batch.get_or_try_init(|| {
            if self.config.require_files {
                self.build_active_files_batch()
            } else {
                self.build_empty_files_batch()
            }
        })
    }

    pub fn pruning_stats(&self) -> DeltaResult<SnapshotPruningStats<'_>> {
        SnapshotPruningStats::try_new(self.files_batch()?, self)
    }

    pub async fn all_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        Ok(self
            .tombstones(log_store)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter())
    }

    pub async fn unexpired_tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> DeltaResult<impl Iterator<Item = Remove>> {
        let retention_timestamp = Utc::now().timestamp_millis()
            - self
                .table_properties()
                .deleted_file_retention_duration()
                .as_millis() as i64;
        let tombstones = self.all_tombstones(log_store).await?.collect::<Vec<_>>();
        Ok(tombstones
            .into_iter()
            .filter(move |t| t.deletion_timestamp.unwrap_or(0) > retention_timestamp))
    }

    pub fn add_actions_table(&self, flatten: bool) -> Result<RecordBatch, DeltaTableError> {
        let actions = self.files_batch()?;
        let mut fields: Vec<FieldRef> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        push_renamed_column(
            actions,
            FIELD_NAME_PATH,
            FIELD_NAME_PATH,
            &mut fields,
            &mut columns,
        )?;
        push_renamed_column(
            actions,
            FIELD_NAME_SIZE,
            "size_bytes",
            &mut fields,
            &mut columns,
        )?;
        push_renamed_column(
            actions,
            FIELD_NAME_MODIFICATION_TIME,
            "modification_time",
            &mut fields,
            &mut columns,
        )?;

        if let Some(stats) = struct_column(actions, FIELD_NAME_STATS_PARSED) {
            let (num_records, nullable) = required_struct_child(stats, STATS_FIELD_NUM_RECORDS)?;
            fields.push(Arc::new(Field::new(
                "num_records",
                num_records.data_type().clone(),
                nullable,
            )));
            columns.push(num_records);

            if let Some((null_count, nullable)) =
                optional_struct_child(stats, STATS_FIELD_NULL_COUNT)
            {
                fields.push(Arc::new(Field::new(
                    "null_count",
                    null_count.data_type().clone(),
                    nullable,
                )));
                columns.push(null_count);
            }
            if let Some((min_values, nullable)) =
                optional_struct_child(stats, STATS_FIELD_MIN_VALUES)
            {
                fields.push(Arc::new(Field::new(
                    "min",
                    min_values.data_type().clone(),
                    nullable,
                )));
                columns.push(min_values);
            }
            if let Some((max_values, nullable)) =
                optional_struct_child(stats, STATS_FIELD_MAX_VALUES)
            {
                fields.push(Arc::new(Field::new(
                    "max",
                    max_values.data_type().clone(),
                    nullable,
                )));
                columns.push(max_values);
            }
        }

        if !self.metadata().partition_columns().is_empty() {
            push_renamed_column(
                actions,
                FIELD_NAME_PARTITION_VALUES_PARSED,
                "partition",
                &mut fields,
                &mut columns,
            )?;
        }

        let result = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
        if flatten {
            Ok(result.normalize(".", None)?)
        } else {
            Ok(result)
        }
    }

    pub(crate) fn tombstones(
        &self,
        _log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<Remove>> {
        futures::stream::iter(self.removes.iter().cloned().map(Ok::<_, DeltaTableError>)).boxed()
    }

    pub fn transaction_version(&self, app_id: impl ToString) -> DeltaResult<Option<i64>> {
        Ok(self
            .app_txns
            .get(&app_id.to_string())
            .map(|txn| txn.version))
    }
}

fn push_renamed_column(
    batch: &RecordBatch,
    input_name: &str,
    output_name: &str,
    fields: &mut Vec<FieldRef>,
    columns: &mut Vec<ArrayRef>,
) -> DeltaResult<()> {
    let schema = batch.schema();
    let index = schema.index_of(input_name).map_err(|_| {
        DeltaTableError::schema(format!("column {input_name} not found in add actions"))
    })?;
    let field = schema.field(index);
    fields.push(Arc::new(Field::new(
        output_name,
        field.data_type().clone(),
        field.is_nullable(),
    )));
    columns.push(batch.column(index).clone());
    Ok(())
}

fn struct_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StructArray> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StructArray>())
}

fn required_struct_child(
    array: &StructArray,
    name: &str,
) -> Result<(ArrayRef, bool), DeltaTableError> {
    optional_struct_child(array, name)
        .ok_or_else(|| DeltaTableError::schema(format!("{name} field not found in struct column")))
}

fn optional_struct_child(array: &StructArray, name: &str) -> Option<(ArrayRef, bool)> {
    let column = array.column_by_name(name)?.clone();
    let nullable = array
        .fields()
        .iter()
        .find(|f| f.name() == name)
        .map(|f| f.is_nullable())
        .unwrap_or(true);
    Some((column, nullable))
}
