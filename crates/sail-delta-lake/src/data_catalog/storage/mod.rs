//! listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use deltalake::errors::DeltaResult;
use deltalake::logstore::{store_for, StorageConfig};
use deltalake::table::builder::ensure_table_uri;
use futures::TryStreamExt;
use object_store::ObjectStore;

const DELTA_LOG_FOLDER: &str = "_delta_log";

/// A `SchemaProvider` that scans an `ObjectStore` to automatically discover delta tables.
///
/// A subfolder relationship is assumed, i.e. given:
/// authority = s3://host.example.com:3000
/// path = /data/tpch
///
/// A table called "customer" will be registered for the folder:
/// s3://host.example.com:3000/data/tpch/customer
///
/// assuming it contains valid deltalake data, i.e a `_delta_log` folder:
/// s3://host.example.com:3000/data/tpch/customer/_delta_log/
#[derive(Debug)]
pub struct ListingSchemaProvider {
    authority: String,
    /// Underlying object store - this should be injected from sail's registry
    store: Arc<dyn ObjectStore>,
    /// A map of table names to a fully qualified storage location
    tables: DashMap<String, String>,
    /// Options used for Delta table operations (not for ObjectStore creation)
    storage_options: StorageConfig,
}

impl ListingSchemaProvider {
    /// Create a new [`ListingSchemaProvider`] with an injected ObjectStore
    ///
    /// This is the preferred constructor that accepts an already-created ObjectStore
    /// from sail's DynamicObjectStoreRegistry, following the dependency injection pattern.
    pub fn new_with_object_store(
        authority: String,
        store: Arc<dyn ObjectStore>,
        storage_options: Option<HashMap<String, String>>,
    ) -> DeltaResult<Self> {
        let storage_options = storage_options.unwrap_or_default();
        Ok(Self {
            authority,
            store,
            tables: DashMap::new(),
            storage_options: StorageConfig::parse_options(storage_options)?,
        })
    }

    /// Create a new [`ListingSchemaProvider`] (legacy constructor)
    ///
    /// This constructor creates its own ObjectStore, which goes against the dependency
    /// injection pattern. It's kept for backward compatibility but should be avoided
    /// in favor of `new_with_object_store`.
    #[deprecated(note = "Use new_with_object_store for better dependency injection")]
    pub fn try_new(
        root_uri: impl AsRef<str>,
        options: Option<HashMap<String, String>>,
    ) -> DeltaResult<Self> {
        let uri = ensure_table_uri(root_uri)?;
        let options = options.unwrap_or_default();
        let store = store_for(&uri, &options)?;
        Ok(Self {
            authority: uri.to_string(),
            store,
            tables: DashMap::new(),
            storage_options: StorageConfig::parse_options(options)?,
        })
    }

    /// Reload table information from ObjectStore
    pub async fn refresh(&self) -> datafusion::common::Result<()> {
        let entries: Vec<_> = self.store.list(None).try_collect().await?;
        let mut tables = HashSet::new();
        for file in entries.iter() {
            let mut parent = Path::new(file.location.as_ref());
            while let Some(p) = parent.parent() {
                if parent.ends_with(DELTA_LOG_FOLDER) {
                    tables.insert(p);
                    break;
                }
                parent = p;
            }
        }
        for table in tables.into_iter() {
            let table_name = normalize_table_name(table)?;
            let table_path = table
                .to_str()
                .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
                .to_string();
            if !self.table_exist(&table_name) {
                let table_url = format!("{}/{table_path}", self.authority);
                self.tables.insert(table_name.to_string(), table_url);
            }
        }
        Ok(())
    }
}

// normalizes a path fragment to be a valid table name in datafusion
// - removes some reserved characters (-, +, ., " ")
// - lowercase ascii
fn normalize_table_name(path: &Path) -> Result<String, DataFusionError> {
    Ok(path
        .file_name()
        .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
        .to_str()
        .ok_or_else(|| DataFusionError::Internal("Cannot parse file name!".to_string()))?
        .replace(['-', '.', ' '], "_")
        .to_ascii_lowercase())
}

#[async_trait]
impl SchemaProvider for ListingSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.key().clone()).collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let Some(location) = self.tables.get(name).map(|t| t.clone()) else {
            return Ok(None);
        };

        // Parse the location to ensure it's a valid URL
        // let table_url =
        //     Url::parse(&location).map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Use sail-delta-lake's open_table_with_object_store to bypass delta-rs internal ObjectStore creation
        // This follows the dependency injection pattern by using the injected ObjectStore
        let delta_table = crate::open_table_with_object_store(
            location,
            self.store.clone(),
            self.storage_options.clone(),
        )
        .await
        .map_err(crate::delta_datafusion::delta_to_datafusion_error)?;

        let config = crate::delta_datafusion::DeltaScanConfig::default();
        let provider = crate::delta_datafusion::DeltaTableProvider::try_new(
            delta_table
                .snapshot()
                .map_err(crate::delta_datafusion::delta_to_datafusion_error)?
                .clone(),
            delta_table.log_store(),
            config,
        )
        .map_err(crate::delta_datafusion::delta_to_datafusion_error)?;

        Ok(Some(Arc::new(provider)))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support registering tables".to_owned(),
        ))
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support deregistering tables".to_owned(),
        ))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
