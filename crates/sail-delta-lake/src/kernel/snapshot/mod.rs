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
use serde_json::Value;
use url::Url;

use crate::kernel::checkpoints::{
    latest_replayable_version, load_replayed_table_header, load_replayed_table_state,
    ReplayedTableState,
};
use crate::kernel::log_segment::ReplayedTableHeader;
pub use crate::kernel::snapshot::stats::SnapshotPruningStats;
use crate::kernel::{DeltaTableConfig, SchemaRef};
use crate::schema::{arrow_field_physical_name, arrow_schema_reorder_partitions};
use crate::spec::fields::{
    FIELD_NAME_MODIFICATION_TIME, FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_PATH,
    FIELD_NAME_SIZE, FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
    STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{
    Add, ColumnMappingMode, ColumnMetadataKey, CommitConflictError, DeltaError as DeltaTableError,
    DeltaResult, DomainMetadata, Metadata, Protocol, Remove, TableFeature, TableProperties,
    Transaction, TransactionError, VersionChecksum,
};
use crate::storage::LogStore;
use crate::table::{
    ChangeDataFeedSupport, ChangeDataFeedToken, ColumnMappingToken, DeletionVectorToken,
    EnabledRowTrackingToken, RowTrackingToken, SupportedRowTrackingToken,
};

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
    domain_metadata: Arc<HashMap<String, DomainMetadata>>,
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
            domain_metadata: Arc::clone(&self.domain_metadata),
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
            replayed.domain_metadata,
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
            domain_metadata: replayed.domain_metadata,
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
        domain_metadata: Vec<DomainMetadata>,
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
            domain_metadata: Arc::new(
                domain_metadata
                    .into_iter()
                    .map(|domain| (domain.domain.clone(), domain))
                    .collect(),
            ),
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
            domain_metadata: Arc::clone(&self.domain_metadata),
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

    pub fn in_commit_timestamps_enabled(&self) -> bool {
        self.protocol()
            .is_in_commit_timestamps_enabled(self.table_properties())
    }

    pub fn in_commit_timestamp_enablement(&self) -> Option<(i64, i64)> {
        self.in_commit_timestamps_enabled()
            .then(|| self.table_properties().in_commit_timestamp_enablement())
            .flatten()
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

    pub fn domain_metadata(&self) -> &HashMap<String, DomainMetadata> {
        self.domain_metadata.as_ref()
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

    pub fn ensure_data_read_supported(&self) -> DeltaResult<()> {
        crate::kernel::transaction::PROTOCOL
            .can_read_from_protocol(self.protocol())
            .map_err(map_read_protocol_error)
    }

    fn has_unsupported_table_features(&self) -> bool {
        if self
            .protocol()
            .reader_features()
            .into_iter()
            .flatten()
            .chain(self.protocol().writer_features().into_iter().flatten())
            .any(|feature| matches!(feature, TableFeature::Unknown))
        {
            return true;
        }

        let reader_unsupported = crate::kernel::transaction::PROTOCOL
            .unsupported_reader_features(self.protocol())
            .map(|features| !features.is_empty())
            .unwrap_or(true);
        let writer_unsupported = crate::kernel::transaction::PROTOCOL
            .unsupported_writer_features(self.protocol())
            .map(|features| !features.is_empty())
            .unwrap_or(true);
        reader_unsupported || writer_unsupported
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
        if self.has_unsupported_table_features() {
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
        let mut domain_metadata = self.domain_metadata.values().cloned().collect::<Vec<_>>();
        domain_metadata.sort_by(|left, right| left.domain.cmp(&right.domain));

        Ok(Some(VersionChecksum {
            txn_id,
            table_size_bytes,
            num_files,
            num_metadata: 1,
            num_protocol: 1,
            in_commit_timestamp_opt,
            set_transactions: (!set_transactions.is_empty()).then_some(set_transactions),
            domain_metadata: (!domain_metadata.is_empty()).then_some(domain_metadata),
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

    // ── Capability token verification ─────────────────────────────────────────

    /// Verify that Column Mapping is active and return a [`ColumnMappingToken`].
    ///
    /// # Errors
    /// Returns [`DeltaError`] if `delta.columnMapping.mode` is `none` (the default).
    pub fn verify_column_mapping(&self) -> DeltaResult<ColumnMappingToken> {
        let mode = self.effective_column_mapping_mode();
        if matches!(mode, ColumnMappingMode::None) {
            return Err(DeltaTableError::generic(
                "column mapping is not enabled on this table (delta.columnMapping.mode = none)",
            ));
        }
        Ok(ColumnMappingToken { mode })
    }

    /// Verify that Deletion Vectors are fully enabled for reads and writes and return a
    /// [`DeletionVectorToken`].
    ///
    /// # Errors
    /// Returns [`DeltaError`] if the protocol does not declare the `deletionVectors`
    /// reader and writer features, or if `delta.enableDeletionVectors` is not `true`.
    pub fn verify_deletion_vectors(&self) -> DeltaResult<DeletionVectorToken> {
        crate::table::features::require_reader_writer_feature(
            self,
            &TableFeature::DeletionVectors,
            "deletionVectors",
        )?;

        let enabled = self
            .metadata()
            .configuration()
            .get("delta.enableDeletionVectors")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if enabled {
            Ok(DeletionVectorToken)
        } else {
            Err(DeltaTableError::generic(
                "Deletion Vectors are not enabled on this table \
                 (set delta.enableDeletionVectors = true)",
            ))
        }
    }

    /// Classify Change Data Feed protocol support for this snapshot.
    ///
    /// CDF spans both legacy protocol versions and the newer writer-feature model, so
    /// protocol support must be tracked separately from the current table property.
    pub fn change_data_feed_support(&self) -> ChangeDataFeedSupport {
        match self.protocol().min_writer_version() {
            0..=3 => ChangeDataFeedSupport::Unsupported,
            4..=6 => ChangeDataFeedSupport::Legacy,
            _ if self
                .protocol()
                .has_writer_feature(&TableFeature::ChangeDataFeed) =>
            {
                ChangeDataFeedSupport::WriterFeature
            }
            _ => ChangeDataFeedSupport::Unsupported,
        }
    }

    /// Verify that Change Data Feed is active on the current snapshot and return a
    /// [`ChangeDataFeedToken`].
    ///
    /// # Errors
    /// Returns [`DeltaError`] if the current protocol does not support CDF or if
    /// `delta.enableChangeDataFeed` is not `true` in the table properties.
    pub fn verify_change_data_feed(&self) -> DeltaResult<ChangeDataFeedToken> {
        match self.change_data_feed_support() {
            ChangeDataFeedSupport::Unsupported if self.protocol().min_writer_version() <= 3 => {
                return Err(DeltaTableError::generic(
                    "Change Data Feed requires protocol minWriterVersion >= 4",
                ));
            }
            ChangeDataFeedSupport::Unsupported => {
                return Err(DeltaTableError::generic(
                    "Change Data Feed requires the changeDataFeed writer feature on writer version >= 7",
                ));
            }
            ChangeDataFeedSupport::Legacy | ChangeDataFeedSupport::WriterFeature => {}
        }

        let enabled = self
            .metadata()
            .configuration()
            .get("delta.enableChangeDataFeed")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        if enabled {
            Ok(ChangeDataFeedToken)
        } else {
            Err(DeltaTableError::generic(
                "Change Data Feed is not enabled on this table \
                 (set delta.enableChangeDataFeed = true)",
            ))
        }
    }

    /// Inspect the Row Tracking state of this snapshot and return the appropriate
    /// [`RowTrackingToken`] variant.
    ///
    /// Callers must match on the returned value to determine whether row-ID assignment
    /// is permitted, suspended, or unsupported.
    pub fn get_row_tracking_state(&self) -> DeltaResult<RowTrackingToken> {
        let config = self.metadata().configuration();
        let tracking_supported = self
            .protocol()
            .has_writer_feature(&TableFeature::RowTracking);
        let domain_metadata_supported = self
            .protocol()
            .has_writer_feature(&TableFeature::DomainMetadata);
        let tracking_enabled = config
            .get("delta.enableRowTracking")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let tracking_suspended = config
            .get("delta.rowTrackingSuspended")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        if tracking_enabled && !tracking_supported {
            return Err(DeltaTableError::generic(
                "delta.enableRowTracking = true requires the rowTracking writer feature",
            ));
        }
        if tracking_supported && !domain_metadata_supported {
            return Err(DeltaTableError::generic(
                "rowTracking requires the domainMetadata writer feature",
            ));
        }
        if tracking_enabled && tracking_suspended {
            return Err(DeltaTableError::generic(
                "delta.enableRowTracking cannot be combined with delta.rowTrackingSuspended = true",
            ));
        }
        if tracking_enabled {
            for key in [
                "delta.rowTracking.materializedRowIdColumnName",
                "delta.rowTracking.materializedRowCommitVersionColumnName",
            ] {
                if config
                    .get(key)
                    .map(|value| value.is_empty())
                    .unwrap_or(true)
                {
                    return Err(DeltaTableError::generic(format!(
                        "{key} is required when delta.enableRowTracking = true"
                    )));
                }
            }
            if self
                .adds
                .iter()
                .any(|add| add.base_row_id.is_none() || add.default_row_commit_version.is_none())
            {
                return Err(DeltaTableError::generic(
                    "enabled row tracking requires all active Add actions to carry baseRowId and defaultRowCommitVersion",
                ));
            }
        }

        let next_row_id = self
            .row_tracking_high_water_mark()?
            .map(|value| value.saturating_add(1))
            .unwrap_or(0);

        if tracking_supported && tracking_suspended {
            Ok(RowTrackingToken::Suspended)
        } else if tracking_enabled {
            Ok(RowTrackingToken::Enabled(EnabledRowTrackingToken {
                next_row_id,
            }))
        } else if tracking_supported {
            Ok(RowTrackingToken::SupportedOnly(SupportedRowTrackingToken {
                next_row_id,
            }))
        } else {
            Ok(RowTrackingToken::Unsupported)
        }
    }

    fn row_tracking_high_water_mark(&self) -> DeltaResult<Option<i64>> {
        let Some(domain) = self.domain_metadata().get("delta.rowTracking") else {
            return Ok(None);
        };
        let configuration: Value = serde_json::from_str(&domain.configuration)?;
        let value = configuration
            .as_object()
            .and_then(|object| object.get("rowIdHighWaterMark"))
            .ok_or_else(|| {
                DeltaTableError::generic(
                    "delta.rowTracking domain metadata is missing rowIdHighWaterMark",
                )
            })?;
        match value {
            Value::Number(number) => number.as_i64().ok_or_else(|| {
                DeltaTableError::generic(
                    "delta.rowTracking rowIdHighWaterMark must be representable as i64",
                )
            }),
            Value::String(string) => string.parse::<i64>().map_err(|_| {
                DeltaTableError::generic(
                    "delta.rowTracking rowIdHighWaterMark must be an integer string",
                )
            }),
            _ => Err(DeltaTableError::generic(
                "delta.rowTracking rowIdHighWaterMark must be a JSON number or string",
            )),
        }
        .map(Some)
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

fn map_read_protocol_error(err: TransactionError) -> DeltaTableError {
    match err {
        TransactionError::UnsupportedTableFeatures(features) => DeltaTableError::Unsupported(
            format!("Reading this Delta table requires unsupported reader features: {features:?}"),
        ),
        TransactionError::CommitConflict(CommitConflictError::UnsupportedReaderVersion(
            version,
        )) => DeltaTableError::Unsupported(format!(
            "Sail Delta Lake does not support reader version {version}"
        )),
        other => DeltaTableError::Transaction(other),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use once_cell::sync::OnceCell;
    use url::Url;

    use super::DeltaSnapshot;
    use crate::datasource::{DeltaScanConfig, DeltaTableProvider};
    use crate::kernel::DeltaTableConfig;
    use crate::logical::table_source::DeltaTableSource;
    use crate::spec::{
        Add, ColumnMappingMode, ColumnMetadataKey, DataType, DomainMetadata, Metadata,
        MetadataValue, Protocol, StructField, StructType, TableFeature, TableProperties,
    };
    use crate::storage::{default_logstore, LogStoreRef, StorageConfig};
    use crate::table::{ChangeDataFeedSupport, RowTrackingToken};

    #[expect(clippy::unwrap_used)]
    fn test_metadata(
        configuration: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> Metadata {
        Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG)]).unwrap(),
            Vec::new(),
            0,
            configuration
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
        .unwrap()
    }

    fn test_snapshot(
        protocol: Protocol,
        metadata: Metadata,
        domain_metadata: Vec<DomainMetadata>,
    ) -> DeltaSnapshot {
        test_snapshot_with_adds(protocol, metadata, domain_metadata, Vec::new())
    }

    #[expect(clippy::unwrap_used)]
    fn test_snapshot_with_adds(
        protocol: Protocol,
        metadata: Metadata,
        domain_metadata: Vec<DomainMetadata>,
        adds: Vec<Add>,
    ) -> DeltaSnapshot {
        let table_properties = TableProperties::from(metadata.configuration().iter());
        DeltaSnapshot {
            version: 0,
            table_url: Url::parse("file:///tmp/test-table").unwrap(),
            config: DeltaTableConfig::default(),
            protocol,
            metadata: metadata.clone(),
            table_properties,
            arrow_schema: Arc::new(metadata.parse_schema_arrow().unwrap()),
            adds: Arc::new(adds),
            removes: Arc::new(Vec::new()),
            app_txns: Arc::new(HashMap::new()),
            domain_metadata: Arc::new(
                domain_metadata
                    .into_iter()
                    .map(|domain| (domain.domain.clone(), domain))
                    .collect(),
            ),
            commit_timestamps: Arc::new(BTreeMap::new()),
            files_batch: OnceCell::new(),
        }
    }

    #[expect(clippy::unwrap_used)]
    fn test_log_store() -> LogStoreRef {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        default_logstore(
            store.clone(),
            store,
            &Url::parse("memory:///").unwrap(),
            &StorageConfig,
        )
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn row_tracking_state_uses_protocol_features_and_domain_high_water_mark() {
        let protocol = Protocol::new(
            1,
            7,
            None,
            Some(vec![
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ]),
        );
        let metadata = test_metadata([
            ("delta.enableRowTracking", "true"),
            (
                "delta.rowTracking.materializedRowIdColumnName",
                "_metadata.row_id",
            ),
            (
                "delta.rowTracking.materializedRowCommitVersionColumnName",
                "_metadata.row_commit_version",
            ),
        ]);
        let snapshot = test_snapshot_with_adds(
            protocol,
            metadata,
            vec![DomainMetadata {
                domain: "delta.rowTracking".to_string(),
                configuration: r#"{"rowIdHighWaterMark":42}"#.to_string(),
                removed: false,
            }],
            vec![Add {
                path: "part-000.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 1,
                modification_time: 0,
                data_change: true,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: Some(0),
                default_row_commit_version: Some(0),
                clustering_provider: None,
                commit_version: None,
                commit_timestamp: None,
            }],
        );

        let next_row_id = match snapshot.get_row_tracking_state().unwrap() {
            RowTrackingToken::Enabled(token) => Some(token.next_row_id),
            _ => None,
        };

        assert_eq!(next_row_id, Some(43));
    }

    #[test]
    fn enabled_row_tracking_requires_materialized_column_configuration() {
        let protocol = Protocol::new(
            1,
            7,
            None,
            Some(vec![
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ]),
        );
        let snapshot = test_snapshot(
            protocol,
            test_metadata([("delta.enableRowTracking", "true")]),
            Vec::new(),
        );

        assert!(snapshot.get_row_tracking_state().is_err());
    }

    #[test]
    fn enabled_row_tracking_requires_active_files_to_carry_row_metadata() {
        let protocol = Protocol::new(
            1,
            7,
            None,
            Some(vec![
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ]),
        );
        let metadata = test_metadata([
            ("delta.enableRowTracking", "true"),
            (
                "delta.rowTracking.materializedRowIdColumnName",
                "_metadata.row_id",
            ),
            (
                "delta.rowTracking.materializedRowCommitVersionColumnName",
                "_metadata.row_commit_version",
            ),
        ]);
        let snapshot = test_snapshot_with_adds(
            protocol,
            metadata,
            vec![DomainMetadata {
                domain: "delta.rowTracking".to_string(),
                configuration: r#"{"rowIdHighWaterMark":42}"#.to_string(),
                removed: false,
            }],
            vec![Add {
                path: "part-000.parquet".to_string(),
                partition_values: HashMap::new(),
                size: 1,
                modification_time: 0,
                data_change: true,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: Some(0),
                clustering_provider: None,
                commit_version: None,
                commit_timestamp: None,
            }],
        );

        assert!(snapshot.get_row_tracking_state().is_err());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn verify_column_mapping_uses_effective_mode_from_schema_annotations() {
        let metadata = Metadata::try_new(
            None,
            None,
            StructType::try_new([StructField::not_null("id", DataType::LONG).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-0001".to_string()),
                ),
            ])])
            .unwrap(),
            Vec::new(),
            0,
            HashMap::new(),
        )
        .unwrap();
        let snapshot = test_snapshot(Protocol::new(1, 1, None, None), metadata, Vec::new());

        let token = snapshot.verify_column_mapping().unwrap();

        assert!(matches!(token.mode, ColumnMappingMode::Name));
    }

    #[test]
    fn verify_deletion_vectors_requires_property_and_feature_flags() {
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![TableFeature::DeletionVectors]),
        );
        let enabled = test_snapshot(
            protocol.clone(),
            test_metadata([("delta.enableDeletionVectors", "true")]),
            Vec::new(),
        );
        assert!(enabled.verify_deletion_vectors().is_ok());

        let missing_reader_feature = test_snapshot(
            Protocol::new(1, 7, None, Some(vec![TableFeature::DeletionVectors])),
            test_metadata([("delta.enableDeletionVectors", "true")]),
            Vec::new(),
        );
        assert!(missing_reader_feature.verify_deletion_vectors().is_err());

        let disabled = test_snapshot(protocol, test_metadata([]), Vec::new());
        assert!(disabled.verify_deletion_vectors().is_err());
    }

    #[test]
    fn change_data_feed_support_distinguishes_protocol_modes() {
        let unsupported = test_snapshot(
            Protocol::new(1, 3, None, None),
            test_metadata([]),
            Vec::new(),
        );
        assert_eq!(
            unsupported.change_data_feed_support(),
            ChangeDataFeedSupport::Unsupported
        );

        let legacy = test_snapshot(
            Protocol::new(1, 4, None, None),
            test_metadata([]),
            Vec::new(),
        );
        assert_eq!(
            legacy.change_data_feed_support(),
            ChangeDataFeedSupport::Legacy
        );

        let missing_feature = test_snapshot(
            Protocol::new(1, 7, None, Some(vec![])),
            test_metadata([]),
            Vec::new(),
        );
        assert_eq!(
            missing_feature.change_data_feed_support(),
            ChangeDataFeedSupport::Unsupported
        );

        let writer_feature = test_snapshot(
            Protocol::new(1, 7, None, Some(vec![TableFeature::ChangeDataFeed])),
            test_metadata([]),
            Vec::new(),
        );
        assert_eq!(
            writer_feature.change_data_feed_support(),
            ChangeDataFeedSupport::WriterFeature
        );
    }

    #[test]
    fn verify_change_data_feed_respects_protocol_generation() {
        let unsupported_legacy = test_snapshot(
            Protocol::new(1, 3, None, None),
            test_metadata([("delta.enableChangeDataFeed", "true")]),
            Vec::new(),
        );
        assert!(unsupported_legacy.verify_change_data_feed().is_err());

        let supported_legacy = test_snapshot(
            Protocol::new(1, 4, None, None),
            test_metadata([("delta.enableChangeDataFeed", "true")]),
            Vec::new(),
        );
        assert!(supported_legacy.verify_change_data_feed().is_ok());

        let protocol_without_feature = Protocol::new(1, 7, None, Some(vec![]));
        let snapshot = test_snapshot(
            protocol_without_feature,
            test_metadata([("delta.enableChangeDataFeed", "true")]),
            Vec::new(),
        );
        assert!(snapshot.verify_change_data_feed().is_err());

        let protocol_with_feature =
            Protocol::new(1, 7, None, Some(vec![TableFeature::ChangeDataFeed]));
        let supported = test_snapshot(
            protocol_with_feature,
            test_metadata([("delta.enableChangeDataFeed", "true")]),
            Vec::new(),
        );
        assert!(supported.verify_change_data_feed().is_ok());
    }

    #[test]
    fn data_read_support_allows_writer_only_features() {
        let protocol = Protocol::new(
            1,
            7,
            None,
            Some(vec![
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ]),
        );
        let snapshot = test_snapshot(protocol, test_metadata([]), Vec::new());

        assert!(snapshot.ensure_data_read_supported().is_ok());
    }

    #[test]
    fn delta_table_provider_rejects_unsupported_reader_features() {
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![TableFeature::DeletionVectors]),
        );
        let snapshot = Arc::new(test_snapshot(protocol, test_metadata([]), Vec::new()));

        let result =
            DeltaTableProvider::try_new(snapshot, test_log_store(), DeltaScanConfig::default());
        assert!(
            result.is_err(),
            "provider creation should reject unsupported reader features"
        );
        let err = match result {
            Err(err) => err,
            Ok(_) => return,
        };

        assert!(matches!(
            err,
            crate::spec::DeltaError::Unsupported(message) if message.contains("DeletionVectors")
        ));
    }

    #[test]
    fn delta_table_source_rejects_unsupported_reader_features() {
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![TableFeature::DeletionVectors]),
        );
        let snapshot = Arc::new(test_snapshot(protocol, test_metadata([]), Vec::new()));

        let result =
            DeltaTableSource::try_new(snapshot, test_log_store(), DeltaScanConfig::default());
        assert!(
            result.is_err(),
            "table source creation should reject unsupported reader features"
        );
        let err = match result {
            Err(err) => err,
            Ok(_) => return,
        };

        assert!(matches!(
            err,
            crate::spec::DeltaError::Unsupported(message) if message.contains("DeletionVectors")
        ));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn version_checksum_skips_explicitly_known_but_unsupported_features() {
        // TODO: support VacuumProtocolCheck
        let protocol = Protocol::new(1, 7, None, Some(vec![TableFeature::VacuumProtocolCheck]));
        let snapshot = test_snapshot(protocol, test_metadata([]), Vec::new());

        assert!(snapshot
            .build_version_checksum(None, None)
            .unwrap()
            .is_none());
    }
}
