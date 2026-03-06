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

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use url::Url;

use crate::kernel::checkpoints::{latest_replayable_version, load_replayed_table_state};
use crate::kernel::snapshot::iterators::LogicalFileView;
pub use crate::kernel::snapshot::log_data::LogDataHandler;
use crate::kernel::{DeltaTableConfig, PredicateRef, SchemaRef};
use crate::spec::{
    Add, ColumnMappingMode, DeltaError as DeltaTableError, DeltaResult, Metadata, Protocol, Remove,
    TableProperties, Transaction,
};
use crate::storage::LogStore;

mod deletion_vector;
mod file_actions;
pub mod iterators;
pub mod log_data;
mod materialize;
mod stats;

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
                Err(crate::spec::DeltaError::MissingVersion) => {
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

    /// Well known properties of the table.
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration.table_properties()
    }

    pub fn table_configuration(&self) -> &SnapshotTableConfiguration {
        &self.table_configuration
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
        let mut eager = Self {
            snapshot,
            files: RecordBatch::new_empty(Arc::new(ArrowSchema::empty())),
        };
        eager.refresh_files()?;
        Ok(eager)
    }

    /// Update the snapshot to the given version.
    pub(crate) async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        self.snapshot.update(log_store, target_version).await?;
        self.refresh_files()
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
