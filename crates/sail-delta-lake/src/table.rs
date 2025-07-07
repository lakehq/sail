//! Custom table opening logic for sail-delta-lake
//!
//! This module provides functions to open Delta tables with external ObjectStore instances,
//! bypassing deltalake's internal ObjectStore creation logic.

use std::sync::Arc;

use deltalake::logstore::{default_logstore, LogStoreRef, StorageConfig};
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use object_store::ObjectStore;
use url::Url;

use crate::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};

/// Opens a Delta table using an external ObjectStore instance.
///
/// This function bypasses deltalake's internal ObjectStore creation logic by directly
/// injecting the provided ObjectStore into a LogStore. This is useful when you want
/// to use a custom ObjectStore (e.g., from sail's registry) instead of letting
/// deltalake create one based on the URL scheme.
///
/// # Arguments
///
/// * `table_uri` - The URI of the Delta table (e.g., "s3://bucket/path/to/table")
/// * `object_store` - The ObjectStore instance to use for accessing the table data
/// * `storage_options` - Additional storage configuration options
///
/// # Returns
///
/// A `DeltaResult<DeltaTable>` containing the loaded Delta table.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use object_store::local::LocalFileSystem;
/// use sail_delta_lake::open_table_with_object_store;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let object_store = Arc::new(LocalFileSystem::new());
/// let table = open_table_with_object_store(
///     "file:///path/to/delta/table",
///     object_store,
///     Default::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
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

/// Creates a new Delta table using an external ObjectStore instance.
///
/// This function creates a new Delta table at the specified location using the provided
/// ObjectStore instance, following the dependency injection pattern. This is useful when
/// you want to ensure that the table creation uses the same ObjectStore configuration
/// as the rest of your application.
///
/// # Arguments
///
/// * `table_uri` - The URI where the Delta table should be created
/// * `object_store` - The ObjectStore instance to use for creating the table
/// * `storage_options` - Additional storage configuration options
///
/// # Returns
///
/// A `DeltaResult<deltalake::DeltaOps>` that can be used to configure and create the table.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use object_store::local::LocalFileSystem;
/// use sail_delta_lake::create_delta_table_with_object_store;
/// use deltalake::kernel::{StructField, DataType, PrimitiveType};
/// use deltalake::protocol::SaveMode;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let object_store = Arc::new(LocalFileSystem::new());
/// let delta_ops = create_delta_table_with_object_store(
///     "file:///path/to/new/delta/table",
///     object_store,
///     Default::default(),
/// ).await?;
///
/// // Configure and create the table
/// let table = delta_ops
///     .create()
///     .with_columns(vec![
///         StructField::new("id".to_string(), DataType::Primitive(PrimitiveType::Long), false),
///         StructField::new("name".to_string(), DataType::Primitive(PrimitiveType::String), true),
///     ])
///     .with_save_mode(SaveMode::ErrorIfExists)
///     .await?;
/// # Ok(())
/// # }
/// ```
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
    // For most cases, we use the same object store for both prefixed and root access
    // The storage_config.decorate_store method will handle any necessary path prefixing
    let prefixed_store =
        storage_config.decorate_store(Arc::clone(&object_store), &location, None)?;

    // Create the default LogStore with our custom ObjectStore
    let log_store = default_logstore(Arc::new(prefixed_store), &location, &storage_config);

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

/// Convenience function to open a Delta table with default storage options.
///
/// This is equivalent to calling `open_table_with_object_store` with an empty
/// `StorageConfig`.
pub async fn open_table_with_object_store_simple(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
) -> DeltaResult<DeltaTable> {
    open_table_with_object_store(table_uri, object_store, StorageConfig::default()).await
}
