use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::Result;
use deltalake::logstore::{default_logstore, LogStoreRef, StorageConfig};
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use object_store::ObjectStore;
use url::Url;

use crate::delta_datafusion::{delta_to_datafusion_error, DeltaScanConfig, DeltaTableProvider};

// TODO: We can accept parsed URL instead of a string when creating Delta tables
//   since `DeltaTableFormat` already handles URL parsing.

pub(crate) async fn open_table_with_object_store(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    let log_store =
        create_logstore_with_object_store(object_store.clone(), location.clone(), storage_options)?;

    let mut table = DeltaTable::new(log_store, Default::default());
    table.load().await?;

    Ok(table)
}

pub(crate) async fn create_delta_table_with_object_store(
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<deltalake::DeltaOps> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    let log_store =
        create_logstore_with_object_store(object_store, location.clone(), storage_options)?;

    let table = DeltaTable::new(log_store, Default::default());
    Ok(deltalake::DeltaOps::from(table))
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
    table_uri: impl AsRef<str>,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    scan_config: Option<DeltaScanConfig>,
) -> DeltaResult<DeltaTableProvider> {
    let table_uri_str = table_uri.as_ref();
    let location = Url::parse(table_uri_str)
        .map_err(|_| DeltaTableError::InvalidTableLocation(table_uri_str.to_string()))?;

    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

    // Load the table state
    let mut table = DeltaTable::new(log_store.clone(), Default::default());
    table.load().await?;

    let snapshot = table.snapshot()?.clone();
    let config = scan_config.unwrap_or_default();

    DeltaTableProvider::try_new(snapshot, log_store, config)
}

pub async fn create_delta_provider(
    ctx: &dyn Session,
    table_uri: &str,
    options: &std::collections::HashMap<String, String>,
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    // TODO: Parse options when needed
    // let resolver = DataSourceOptionsResolver::new(ctx);
    // let delta_options = resolver.resolve_delta_read_options(options.clone())?;
    let _ = options;

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
