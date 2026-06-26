use std::collections::HashMap;
use std::sync::Arc;

use sail_catalog::credentials::CatalogCredentials;
use sail_catalog::error::CatalogResult;
use sail_catalog::lakehouse::{
    BeginTableAccessRequest, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest, LakehouseCreatePlan,
    LakehouseCreateRequest, LakehouseResolvedTable, LakehouseScanPlanningRequest,
    LakehouseScanPlanningResponse, ResolveLakehouseTableRequest, TableAccessSession,
};
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableMetadataRequirement,
    CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions,
    Namespace,
};
use sail_catalog_iceberg::{
    IcebergRestCatalogOptions, IcebergRestCatalogProvider, REST_CATALOG_PROP_PREFIX,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use sail_catalog_unity::{UnityCatalogOptions, UnityCatalogProvider};
use sail_common_datafusion::catalog::{DatabaseStatus, TableKind, TableStatus};
use tokio::sync::OnceCell;

use crate::credentials::OneLakeCredentials;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OneLakeApiKind {
    Delta,
    Iceberg,
}

const ONE_LAKE_DELTA_ENDPOINT: &str = "https://onelake.table.fabric.microsoft.com/delta";
const ONE_LAKE_ICEBERG_ENDPOINT: &str = "https://onelake.table.fabric.microsoft.com/iceberg";

/// Configuration for OneLake catalog.
#[derive(Debug, Clone)]
pub struct OneLakeCatalogConfig {
    #[expect(unused)]
    pub workspace: String,
    pub item_name: String,
    pub item_type: String,
    pub api: OneLakeApiKind,
}

/// Provider for Microsoft Fabric OneLake catalog.
///
/// This catalog uses the OneLake Table APIs and delegates protocol-specific catalog
/// behavior to the Iceberg REST or Unity Catalog providers.
pub struct OneLakeCatalogProvider {
    name: String,
    config: OneLakeCatalogConfig,
    inner: Arc<dyn CatalogProvider>,
}

impl OneLakeCatalogProvider {
    pub fn new(
        name: String,
        workspace: String,
        item_name: String,
        item_type: String,
        api: OneLakeApiKind,
        bearer_token: Option<String>,
    ) -> CatalogResult<Self> {
        let credentials = match bearer_token {
            Some(bearer_token) => OneLakeCredentials::Static { bearer_token },
            None => OneLakeCredentials::Dynamic {
                workspace: workspace.clone(),
                provider: OnceCell::new(),
            },
        };
        let credentials = Arc::new(credentials) as Arc<dyn CatalogCredentials>;
        let item_path = format!("{workspace}/{item_name}.{item_type}");
        let inner: Arc<dyn CatalogProvider> = match api {
            OneLakeApiKind::Delta => {
                let uri = format!(
                    "{}/{}/api/2.1/unity-catalog",
                    ONE_LAKE_DELTA_ENDPOINT, item_path
                );
                let catalog_name = format!("{item_name}.{item_type}");
                Arc::new(UnityCatalogProvider::new(
                    name.clone(),
                    UnityCatalogOptions {
                        default_catalog: catalog_name,
                        uri,
                        credentials,
                        user_agent: Some("Sail".to_string()),
                        quote_object_name: false,
                    },
                )?)
            }
            OneLakeApiKind::Iceberg => {
                let uri = ONE_LAKE_ICEBERG_ENDPOINT.to_string();
                let mut properties = HashMap::new();
                properties.insert(REST_CATALOG_PROP_URI.to_string(), uri);
                properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), item_path.clone());
                properties.insert(REST_CATALOG_PROP_PREFIX.to_string(), item_path);
                Arc::new(IcebergRestCatalogProvider::new(
                    name.clone(),
                    IcebergRestCatalogOptions {
                        credentials,
                        properties,
                        user_agent: Some("Sail".to_string()),
                    },
                ))
            }
        };

        Ok(Self {
            name,
            config: OneLakeCatalogConfig {
                workspace,
                item_name,
                item_type,
                api,
            },
            inner,
        })
    }

    fn catalog_name(&self) -> String {
        format!("{}.{}", self.config.item_name, self.config.item_type)
    }

    fn normalize_database_status(&self, mut status: DatabaseStatus) -> DatabaseStatus {
        if matches!(self.config.api, OneLakeApiKind::Delta) {
            status.database = self.normalize_delta_database(status.database);
        }
        status
    }

    fn normalize_table_status(&self, mut status: TableStatus) -> TableStatus {
        if matches!(self.config.api, OneLakeApiKind::Delta) {
            status.database = self.normalize_delta_database(status.database);
            if let TableKind::Table { location, .. } = &mut status.kind {
                if let Some(url) = location.take() {
                    *location = Some(convert_to_abfss_url(&url).unwrap_or(url));
                }
            }
        }
        status
    }

    fn normalize_delta_database(&self, database: Vec<String>) -> Vec<String> {
        if database.first().is_some_and(|x| x == &self.catalog_name()) {
            database.into_iter().skip(1).collect()
        } else {
            database
        }
    }

    fn normalize_database_statuses(&self, statuses: Vec<DatabaseStatus>) -> Vec<DatabaseStatus> {
        statuses
            .into_iter()
            .map(|status| self.normalize_database_status(status))
            .collect()
    }

    fn normalize_table_statuses(&self, statuses: Vec<TableStatus>) -> Vec<TableStatus> {
        statuses
            .into_iter()
            .map(|status| self.normalize_table_status(status))
            .collect()
    }
}

/// Converts OneLake https:// URL to abfss:// URL for Delta Lake access.
/// From: https://onelake.dfs.fabric.microsoft.com/workspace/item.ItemType/Tables/schema/table
/// To:   abfss://workspace@onelake.dfs.fabric.microsoft.com/item.ItemType/Tables/schema/table
fn convert_to_abfss_url(https_url: &str) -> Option<String> {
    let url = https_url.strip_prefix("https://onelake.dfs.fabric.microsoft.com/")?;
    let (workspace, rest) = url.split_once('/')?;
    Some(format!(
        "abfss://{}@onelake.dfs.fabric.microsoft.com/{}",
        workspace, rest
    ))
}

#[async_trait::async_trait]
impl CatalogProvider for OneLakeCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let status = self.inner.create_database(database, options).await?;
        Ok(self.normalize_database_status(status))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let status = self.inner.get_database(database).await?;
        Ok(self.normalize_database_status(status))
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let statuses = self.inner.list_databases(prefix).await?;
        Ok(self.normalize_database_statuses(statuses))
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_database(database, options).await
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_table(database, table, options).await?;
        Ok(self.normalize_table_status(status))
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        self.inner.create_table_metadata_requirement(options)
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        self.inner.lakehouse_capabilities()
    }

    async fn resolve_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: ResolveLakehouseTableRequest,
    ) -> CatalogResult<LakehouseResolvedTable> {
        self.inner
            .resolve_lakehouse_table(database, table, request)
            .await
    }

    async fn plan_lakehouse_create(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCreateRequest,
    ) -> CatalogResult<LakehouseCreatePlan> {
        self.inner
            .plan_lakehouse_create(database, table, request)
            .await
    }

    async fn begin_table_access(
        &self,
        database: &Namespace,
        table: &str,
        request: BeginTableAccessRequest,
    ) -> CatalogResult<TableAccessSession> {
        self.inner
            .begin_table_access(database, table, request)
            .await
    }

    async fn plan_lakehouse_scan(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseScanPlanningRequest,
    ) -> CatalogResult<LakehouseScanPlanningResponse> {
        self.inner
            .plan_lakehouse_scan(database, table, request)
            .await
    }

    async fn commit_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        self.inner
            .commit_lakehouse_table(database, table, request)
            .await
    }

    async fn get_delta_ratified_commits(
        &self,
        database: &Namespace,
        table: &str,
        request: DeltaRatifiedCommitRequest,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        self.inner
            .get_delta_ratified_commits(database, table, request)
            .await
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let status = self.inner.get_table(database, table).await?;
        Ok(self.normalize_table_status(status))
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let statuses = self.inner.list_tables(database).await?;
        Ok(self.normalize_table_statuses(statuses))
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_table(database, table, options).await
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        self.inner.alter_table(database, table, options).await
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_view(database, view, options).await?;
        Ok(self.normalize_table_status(status))
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let status = self.inner.get_view(database, view).await?;
        Ok(self.normalize_table_status(status))
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let statuses = self.inner.list_views(database).await?;
        Ok(self.normalize_table_statuses(statuses))
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_view(database, view, options).await
    }
}
