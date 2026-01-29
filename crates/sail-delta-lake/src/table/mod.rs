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

// [Credit]: https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/mod.rs

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::Result;
use delta_kernel::Error as KernelError;
use object_store::ObjectStore;
pub use state::DeltaTableState;
use url::Url;

use crate::datasource::{DeltaScanConfig, DeltaTableProvider};
use crate::kernel::{DeltaResult, DeltaTableConfig, DeltaTableError};
use crate::options::TableDeltaOptions;
use crate::storage::{commit_uri_from_version, default_logstore, LogStoreRef, StorageConfig};
mod state;

/// In memory representation of a Delta Table
///
/// A DeltaTable is a purely logical concept that represents a dataset that can evolve over time.
/// To attain concrete information about a table a snapshot need to be loaded.
/// Most commonly this is the latest state of the table, but may also loaded for a specific
/// version or point in time.
#[derive(Clone)]
pub struct DeltaTable {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<DeltaTableState>,
    /// the load options used during load
    pub config: DeltaTableConfig,
    /// log store
    pub(crate) log_store: LogStoreRef,
}

impl DeltaTable {
    /// Create a new Delta Table struct without loading any data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method, please
    /// call one of the `open_table` helper methods instead.
    pub fn new(log_store: LogStoreRef, config: DeltaTableConfig) -> Self {
        Self {
            state: None,
            log_store,
            config,
        }
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(&mut self, version: i64) -> Result<(), DeltaTableError> {
        if let Some(snapshot) = &self.state {
            if snapshot.version() > version {
                self.state = None;
            }
        }
        self.update_incremental(Some(version)).await
    }

    /// Get the timestamp of a given version commit.
    pub(crate) async fn get_version_timestamp(&self, version: i64) -> Result<i64, DeltaTableError> {
        if let Some(ts) = self
            .state
            .as_ref()
            .and_then(|s| s.version_timestamp(version))
        {
            return Ok(ts);
        }

        let commit_uri = commit_uri_from_version(version);
        let meta = self.log_store.object_store(None).head(&commit_uri).await?;
        Ok(meta.last_modified.timestamp_millis())
    }

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        match self.state.as_mut() {
            Some(state) => state.update(self.log_store.as_ref(), max_version).await,
            _ => {
                let state = DeltaTableState::try_new(
                    self.log_store.as_ref(),
                    self.config.clone(),
                    max_version,
                )
                .await?;
                self.state = Some(state);
                Ok(())
            }
        }
    }

    /// Returns the currently loaded state snapshot.
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state
            .as_ref()
            .ok_or_else(|| DeltaTableError::generic("Table has not yet been initialized"))
    }

    /// Currently loaded version of the table - if any.
    pub fn version(&self) -> Option<i64> {
        self.state.as_ref().map(|s| s.version())
    }

    /// The URI of the underlying data
    pub fn table_uri(&self) -> String {
        self.log_store.root_uri()
    }

    /// get a shared reference to the log store
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DeltaTable({})", self.table_uri())?;
        writeln!(f, "\tversion: {:?}", self.version())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_uri())
    }
}

pub async fn open_table_with_object_store(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location, storage_options)?;

    let mut table = DeltaTable::new(log_store, Default::default());
    table.load().await?;

    Ok(table)
}

/// Open and load a Delta table with an explicit kernel load config.
///
/// This is primarily useful for planning-time code paths where we want to avoid eagerly loading
/// file-level metadata on the driver (e.g. `require_files=false`).
pub async fn open_table_with_object_store_and_table_config(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    table_config: DeltaTableConfig,
) -> DeltaResult<DeltaTable> {
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location, storage_options)?;

    let mut table = DeltaTable::new(log_store, table_config);
    table.load().await?;

    Ok(table)
}

pub(crate) async fn create_delta_table_with_object_store(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

    let table = DeltaTable::new(log_store, Default::default());
    Ok(table)
}

fn create_logstore_with_object_store(
    object_store: Arc<dyn ObjectStore>,
    location: Url,
    storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let prefixed_store = storage_config.decorate_store(Arc::clone(&object_store), &location)?;

    let log_store = default_logstore(
        Arc::new(prefixed_store),
        object_store,
        &location,
        &storage_config,
    );

    Ok(log_store)
}

/// Creates a Delta Lake table provider
pub async fn create_delta_provider(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: TableDeltaOptions,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let url = ListingTableUrl::try_new(table_url.clone(), None)?;
    let object_store = ctx.runtime_env().object_store(&url)?;
    let storage_config = StorageConfig;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), storage_config)?;

    // Create a new DeltaTable instance but do not load it yet.
    let mut deltalake_table = DeltaTable::new(log_store.clone(), Default::default());

    // Load the table state according to the provided time travel options.
    load_table_by_options(&mut deltalake_table, &options).await?;

    let snapshot = deltalake_table.snapshot()?.clone();

    let scan_config = DeltaScanConfig {
        file_column_name: None,
        wrap_partition_values: false,
        enable_parquet_pushdown: true,
        schema: match schema {
            Some(ref s) if s.fields().is_empty() => None,
            Some(s) => Some(Arc::new(s)),
            None => None,
        },
        commit_version_column_name: None,
        commit_timestamp_column_name: None,
    };

    let table_provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config)?;

    Ok(Arc::new(table_provider))
}

/// Helper function to load a DeltaTable based on version or timestamp options.
async fn load_table_by_options(table: &mut DeltaTable, options: &TableDeltaOptions) -> Result<()> {
    // Precedence: version > timestamp > latest.
    if let Some(version) = options.version_as_of {
        table.load_version(version).await?;
    } else if let Some(timestamp_str) = &options.timestamp_as_of {
        // This logic is adapted from delta-rs `DeltaTable::load_with_datetime`
        let datetime = DateTime::parse_from_rfc3339(timestamp_str)
            .map_err(|e| DeltaTableError::generic(format!("Invalid timestamp string: {}", e)))?
            .with_timezone(&Utc);

        let target_version = find_version_for_timestamp(table, datetime)
            .await
            .map_err(|e| {
                if matches!(e, DeltaTableError::Kernel(KernelError::MissingVersion)) {
                    DeltaTableError::generic(format!(
                        "No version of the Delta table exists at or before timestamp {}",
                        timestamp_str
                    ))
                } else {
                    e
                }
            })?;

        table.load_version(target_version).await?;
    } else {
        // Default behavior: load the latest version.
        table.load().await?;
    }
    Ok(())
}

/// Finds the latest version of the table that was committed at or before a given timestamp.
async fn find_version_for_timestamp(
    table: &mut DeltaTable,
    datetime: DateTime<Utc>,
) -> DeltaResult<i64> {
    let log_store = table.log_store();
    let mut max_version = log_store.get_latest_version(0).await?;
    let mut min_version = 0;

    // In case the table is not initialized yet (e.g. state is None),
    // get_version_timestamp needs some state to work with. Let's load version 0.
    if table.version().is_none() {
        table.load_version(0).await?;
    }

    let target_ts = datetime.timestamp_millis();
    let mut target_version = -1;

    // Binary search to find the correct version
    while min_version <= max_version {
        let pivot = min_version + (max_version - min_version) / 2;
        let pivot_ts = table.get_version_timestamp(pivot).await?;

        if pivot_ts <= target_ts {
            // This version is a potential candidate, try to find a newer one
            target_version = pivot;
            min_version = pivot + 1;
        } else {
            // This version is too new, search in older versions
            max_version = pivot - 1;
        }
    }

    if target_version == -1 {
        // If no version was found, it means the provided timestamp is before the first commit.
        Err(KernelError::MissingVersion.into())
    } else {
        Ok(target_version)
    }
}
