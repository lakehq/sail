// SPDX-License-Identifier: Apache-2.0

//! Catalogs for the tests of this crate and of the crates that enable the `test-util` feature.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sail_common_datafusion::catalog::delta::DELTA_UNITY_TABLE_ID_KEY;
use sail_common_datafusion::catalog::{
    DatabaseStatus, StorageAccess, StorageCredential, TableKind, TableStatus,
    TableStorageCredentials,
};

use crate::error::{CatalogError, CatalogResult};
use crate::lakehouse::{LakehouseCapability, TableStorageCredentialsRequest};
use crate::manager::{CatalogManager, CatalogManagerOptions};
use crate::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};

/// The answer of a catalog to a request for the storage credentials of a table.
pub type StorageCredentialsAnswer = fn() -> CatalogResult<Option<TableStorageCredentials>>;

/// A catalog named `unity` in which every table is a managed Delta table at the same
/// location whose commits the catalog ratifies. The catalog answers the requests for
/// the storage credentials of a table with `credentials` and records them.
pub struct ManagedDeltaCatalog {
    pub location: String,
    pub credentials: StorageCredentialsAnswer,
    /// The qualified table name and the request, in the order of the requests.
    pub credential_requests: Mutex<Vec<(String, TableStorageCredentialsRequest)>>,
}

impl ManagedDeltaCatalog {
    /// A catalog that does not issue storage credentials.
    pub fn new(location: &str) -> Self {
        Self::with_credentials(location, || Ok(None))
    }

    /// A catalog that issues storage credentials that do not expire.
    pub fn vending(location: &str) -> Self {
        Self::with_credentials(location, issued_storage_credentials)
    }

    pub fn with_credentials(location: &str, credentials: StorageCredentialsAnswer) -> Self {
        Self {
            location: location.to_string(),
            credentials,
            credential_requests: Mutex::new(Vec::new()),
        }
    }
}

/// Storage credentials that a catalog issued for reading and that do not expire.
pub fn issued_storage_credentials() -> CatalogResult<Option<TableStorageCredentials>> {
    Ok(Some(TableStorageCredentials {
        credential: StorageCredential::GcsOauth {
            token: "secret-token".to_string(),
        },
        expires_at_ms: None,
        access: StorageAccess::Read,
    }))
}

/// A catalog manager with the catalog as the only catalog and `default` as the database.
pub fn single_catalog_manager(catalog: Arc<dyn CatalogProvider>) -> CatalogResult<CatalogManager> {
    let name = catalog.get_name().to_string();
    CatalogManager::try_new(CatalogManagerOptions {
        catalogs: std::iter::once((name.clone(), catalog)).collect(),
        default_catalog: name,
        default_database: vec!["default".to_string()],
        global_temporary_database: vec!["global_temp".to_string()],
    })
}

fn unsupported<T>() -> CatalogResult<T> {
    Err(CatalogError::NotSupported(
        "unused test catalog operation".to_string(),
    ))
}

#[async_trait]
impl CatalogProvider for ManagedDeltaCatalog {
    fn get_name(&self) -> &str {
        "unity"
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        vec![LakehouseCapability::DeltaRatifiedCommits]
    }

    async fn get_table_storage_credentials(
        &self,
        database: &Namespace,
        table: &str,
        request: TableStorageCredentialsRequest,
    ) -> CatalogResult<Option<TableStorageCredentials>> {
        let database = Vec::<String>::from(database.clone()).join(".");
        self.credential_requests
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))?
            .push((format!("{database}.{table}"), request));
        (self.credentials)()
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        Ok(DatabaseStatus {
            catalog: self.get_name().to_string(),
            database: database.clone().into(),
            comment: None,
            location: None,
            properties: vec![],
        })
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        Ok(TableStatus {
            catalog: Some(self.get_name().to_string()),
            database: database.clone().into(),
            name: table.to_string(),
            kind: TableKind::Table {
                columns: vec![],
                comment: None,
                constraints: vec![],
                location: Some(self.location.clone()),
                format: "delta".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                properties: vec![(DELTA_UNITY_TABLE_ID_KEY.to_string(), "table-id".to_string())],
                is_external: false,
            },
        })
    }

    async fn alter_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: AlterTableOptions,
    ) -> CatalogResult<()> {
        Ok(())
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        unsupported()
    }

    async fn list_databases(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        unsupported()
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        unsupported()
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        unsupported()
    }

    async fn list_tables(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        unsupported()
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        unsupported()
    }

    async fn create_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        unsupported()
    }

    async fn get_view(&self, _database: &Namespace, _view: &str) -> CatalogResult<TableStatus> {
        unsupported()
    }

    async fn list_views(&self, _database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        unsupported()
    }

    async fn drop_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: DropViewOptions,
    ) -> CatalogResult<()> {
        unsupported()
    }
}
