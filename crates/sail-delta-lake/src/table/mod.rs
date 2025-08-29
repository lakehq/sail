use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::Result;
use deltalake::logstore::{default_logstore, LogStoreRef, StorageConfig};
use deltalake::table::builder::DeltaTableConfig;
use deltalake::{DeltaResult, DeltaTableError};
use object_store::ObjectStore;
use url::Url;

// [Credit]: https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/mod.rs
use crate::delta_datafusion::{delta_to_datafusion_error, DeltaScanConfig, DeltaTableProvider};
use crate::options::TableDeltaOptions;

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

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        match self.state.as_mut() {
            Some(state) => state.update(&self.log_store, max_version).await,
            _ => {
                let state =
                    DeltaTableState::try_new(&self.log_store, self.config.clone(), max_version)
                        .await?;
                self.state = Some(state);
                Ok(())
            }
        }
    }

    /// Returns the currently loaded state snapshot.
    pub fn snapshot(&self) -> DeltaResult<&DeltaTableState> {
        self.state.as_ref().ok_or(DeltaTableError::NotInitialized)
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

mod state;
pub use state::DeltaTableState;

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

pub(crate) async fn create_delta_table_provider_with_object_store(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    scan_config: Option<DeltaScanConfig>,
) -> DeltaResult<DeltaTableProvider> {
    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

    // Load the table state using deltalake's DeltaTable
    let mut deltalake_table = DeltaTable::new(log_store.clone(), Default::default());
    deltalake_table.load().await?;

    let snapshot = deltalake_table.snapshot()?.clone();
    let config = scan_config.unwrap_or_default();

    DeltaTableProvider::try_new(snapshot, log_store, config)
}

pub async fn create_delta_provider(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    _options: TableDeltaOptions,
) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let url = ListingTableUrl::try_new(table_url.clone(), None)?;
    let object_store = ctx.runtime_env().object_store(&url)?;

    let storage_config = StorageConfig::default();

    let scan_config = DeltaScanConfig {
        file_column_name: None,
        wrap_partition_values: false, // Default to false for Spark compatibility
        enable_parquet_pushdown: true, // Default to true for now
        schema: match schema {
            Some(ref s) if s.fields().is_empty() => None,
            Some(s) => Some(Arc::new(s)),
            None => None,
        },
    };

    let table_provider = create_delta_table_provider_with_object_store(
        table_url,
        object_store,
        storage_config,
        Some(scan_config),
    )
    .await
    .map_err(delta_to_datafusion_error)?;

    Ok(Arc::new(table_provider))
}

#[allow(dead_code)]
pub(crate) fn get_partition_col_data_types<'a>(
    schema: &'a delta_kernel::schema::StructType,
    metadata: &'a delta_kernel::actions::Metadata,
) -> Vec<(&'a String, &'a delta_kernel::schema::DataType)> {
    // JSON add actions contain a `partitionValues` field which is a map<string, string>.
    // When loading `partitionValues_parsed` we have to convert the stringified partition values back to the correct data type.
    schema
        .fields()
        .filter_map(|f| {
            if metadata
                .partition_columns()
                .iter()
                .any(|s| s.as_str() == f.name())
            {
                Some((f.name(), f.data_type()))
            } else {
                None
            }
        })
        .collect()
}
