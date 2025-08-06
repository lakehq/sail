use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::Result;
use deltalake::logstore::{default_logstore, LogStoreRef, StorageConfig};
use deltalake::{DeltaResult, DeltaTable};
use object_store::ObjectStore;
use url::Url;

use crate::delta_datafusion::{delta_to_datafusion_error, DeltaScanConfig, DeltaTableProvider};

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
) -> DeltaResult<deltalake::DeltaOps> {
    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

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
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    scan_config: Option<DeltaScanConfig>,
) -> DeltaResult<DeltaTableProvider> {
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
    table_url: Url,
    schema: Option<Schema>,
    options: &[std::collections::HashMap<String, String>],
) -> Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
    // TODO: Parse options (with overwrite logic) when needed
    // let resolver = DataSourceOptionsResolver::new(ctx);
    // let delta_options = resolver.resolve_delta_read_options(options.clone())?;
    let _ = options;

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
