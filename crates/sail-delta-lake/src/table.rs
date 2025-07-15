use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::Result;
use deltalake::logstore::{default_logstore, LogStoreRef, StorageConfig};
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use object_store::ObjectStore;
use url::Url;

use crate::delta_datafusion::{delta_to_datafusion_error, DeltaScanConfig, DeltaTableProvider};

pub async fn open_table_with_object_store(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    // Create a LogStore using the provided ObjectStore
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location.clone(), storage_options)?;

    // Create and load the Delta table
    let mut table = DeltaTable::new(log_store, Default::default());
    table.load().await?;

    Ok(table)
}

pub async fn create_delta_table_with_object_store(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<deltalake::DeltaOps> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    // Create a LogStore using the provided ObjectStore
    let log_store =
        create_logstore_with_object_store(object_store, location.clone(), storage_options)?;

    // Create DeltaOps with the injected LogStore
    // This bypasses delta-rs's internal ObjectStore creation
    let table = DeltaTable::new(log_store, Default::default());
    Ok(deltalake::DeltaOps::from(table))
}

/// Creates a LogStore using an external ObjectStore instance.
///
/// This function creates both a prefixed store (for table operations) and uses the same
/// store as the root store. The prefixed store points to the table root, while the root
/// store is used for broader object store operations.
fn create_logstore_with_object_store(
    object_store: Arc<dyn ObjectStore>,
    location: Url,
    storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let prefixed_store = storage_config.decorate_store(Arc::clone(&object_store), &location)?;

    // Create the default LogStore with our custom ObjectStore
    let log_store = default_logstore(
        Arc::new(prefixed_store),
        object_store, // root_store
        &location,
        &storage_config,
    );

    Ok(log_store)
}

/// Creates a DeltaTableProvider using an external ObjectStore instance.
///
/// This function is similar to `open_table_with_object_store` but returns a
/// `DeltaTableProvider` that can be used directly with DataFusion.
///
/// # Arguments
///
/// * `table_uri` - The URI of the Delta table
/// * `object_store` - The ObjectStore instance to use
/// * `storage_options` - Additional storage configuration options
/// * `scan_config` - Configuration for the Delta scan operations
///
/// # Returns
///
/// A `DeltaResult<DeltaTableProvider>` that can be used with DataFusion.
pub async fn create_delta_table_provider_with_object_store(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    scan_config: Option<DeltaScanConfig>,
) -> DeltaResult<DeltaTableProvider> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    // Create a LogStore using the provided ObjectStore
    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

    // Load the table state
    let mut table = DeltaTable::new(log_store.clone(), Default::default());
    table.load().await?;

    let snapshot = table.snapshot()?.clone();
    let config = scan_config.unwrap_or_default();

    // Create the DeltaTableProvider
    DeltaTableProvider::try_new(snapshot, log_store, config)
}

/// Creates a Delta table provider using SessionContext to get the object store.
///
/// This function contains the complete logic for creating a Delta table provider,
/// including parsing the table URI, getting the object store, and configuring the scan.
///
/// # Arguments
///
/// * `ctx` - The DataFusion SessionContext
/// * `table_uri` - The URI of the Delta table
/// * `options` - Additional options for the Delta table (currently unused)
///
/// # Returns
///
/// A `Result<Arc<dyn TableProvider>>` that can be used with DataFusion.
pub async fn create_delta_provider(
    ctx: &datafusion::prelude::SessionContext,
    table_uri: &str,
    options: &std::collections::HashMap<String, String>,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    // TODO: Parse options using DataSourceOptionsResolver when needed
    // let resolver = DataSourceOptionsResolver::new(ctx);
    // let delta_options = resolver.resolve_delta_read_options(options.clone())?;
    let _ = options; // Suppress unused variable warning

    let url = ListingTableUrl::parse(table_uri)?;
    let object_store = ctx.runtime_env().object_store(&url)?;

    let storage_config = StorageConfig::default();

    // Create DeltaScanConfig with proper field names
    let scan_config = DeltaScanConfig {
        file_column_name: None,
        wrap_partition_values: true,
        enable_parquet_pushdown: true, // Default to true for now
        schema: None,
    };

    let table_provider = create_delta_table_provider_with_object_store(
        table_uri,
        object_store,
        storage_config,
        Some(scan_config),
    )
    .await
    .map_err(delta_to_datafusion_error)?;

    Ok(Arc::new(table_provider))
}
