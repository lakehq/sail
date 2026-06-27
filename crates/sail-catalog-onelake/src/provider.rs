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
use url::Url;

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
    pub item_type: Option<String>,
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
        item_type: Option<String>,
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
        // `<workspace>/<item>.<type>` for friendly names, or
        // `<workspaceId>/<itemId>`(no type segment) for GUIDs
        let item_path = onelake_item_path(&workspace, &item_name, item_type.as_deref());
        let inner: Arc<dyn CatalogProvider> = match api {
            OneLakeApiKind::Delta => {
                let uri = format!(
                    "{}/{}/api/2.1/unity-catalog",
                    ONE_LAKE_DELTA_ENDPOINT, item_path
                );
                let catalog_name = onelake_catalog_name(&item_name, item_type.as_deref());
                Arc::new(UnityCatalogProvider::new(
                    name.clone(),
                    UnityCatalogOptions {
                        default_catalog: catalog_name,
                        uri,
                        credentials,
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
        onelake_catalog_name(&self.config.item_name, self.config.item_type.as_deref())
    }

    fn normalize_database_status(&self, mut status: DatabaseStatus) -> DatabaseStatus {
        if matches!(self.config.api, OneLakeApiKind::Delta) {
            status.database = self.normalize_delta_database(status.database);
        }
        // OneLake's Iceberg REST API returns the namespace `location` as a workspace-relative
        // path (e.g. `workspace/item.Type/Tables/schema`). `resolve_default_table_location` then
        // seems to append the table name and hand the result to the Iceberg writer, which seems
        // to reject any non-absolute location, so convert it to an absolute `abfss://` URL.
        if let Some(location) = status.location.take() {
            status.location = Some(normalize_onelake_location(&location).unwrap_or(location));
        }
        // Keep the mirrored `location` table property consistent with the field above
        // (DESCRIBE DATABASE EXTENDED surfaces both).
        for (key, value) in status.properties.iter_mut() {
            if key.eq_ignore_ascii_case("location") {
                if let Some(abfss) = normalize_onelake_location(value) {
                    *value = abfss;
                }
            }
        }
        status
    }

    fn normalize_table_status(&self, mut status: TableStatus) -> TableStatus {
        if matches!(self.config.api, OneLakeApiKind::Delta) {
            status.database = self.normalize_delta_database(status.database);
            if let TableKind::Table { location, .. } = &mut status.kind {
                if let Some(url) = location.take() {
                    *location = Some(normalize_onelake_location(&url).unwrap_or(url));
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

// OneLake reports locations in different forms depending on the API:
// - Delta (Unity) returns an absolute `https://` URL:
//   From: https://onelake.dfs.fabric.microsoft.com/workspace/item.ItemType/Tables/schema/table
//   To:   abfss://workspace@onelake.dfs.fabric.microsoft.com/item.ItemType/Tables/schema/table
// - Iceberg (REST) returns the namespace `location` as a workspace-relative path:
//   From: workspace/item.ItemType/Tables/schema
//   To:   abfss://workspace@onelake.dfs.fabric.microsoft.com/item.ItemType/Tables/schema
fn normalize_onelake_location(location: &str) -> Option<String> {
    let trimmed = location.trim();
    let relative =
        if let Some(rest) = trimmed.strip_prefix("https://onelake.dfs.fabric.microsoft.com/") {
            // OneLake `https://` form.
            rest
        } else if Url::parse(trimmed)
            .ok()
            .is_some_and(|url| url.scheme().len() > 1)
        {
            // Already absolute with another scheme (e.g., `abfss://`, `file://`, ...).
            return None;
        } else {
            // Workspace-relative form (Iceberg namespace location).
            trimmed
        };
    let (workspace, rest) = relative.split_once('/')?;
    (!workspace.is_empty() && !rest.is_empty())
        .then(|| format!("abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{rest}"))
}

// `<workspace>/<item>.<type>` for friendly names, or `<workspaceId>/<itemId>` for GUIDs.
fn onelake_item_path(workspace: &str, item_name: &str, item_type: Option<&str>) -> String {
    match item_type {
        Some(item_type) => format!("{workspace}/{item_name}.{item_type}"),
        None => format!("{workspace}/{item_name}"),
    }
}

// `<item>.<type>` for friendly names, or `<itemId>` for GUIDs.
fn onelake_catalog_name(item_name: &str, item_type: Option<&str>) -> String {
    match item_type {
        Some(item_type) => format!("{item_name}.{item_type}"),
        None => item_name.to_string(),
    }
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

#[cfg(test)]
mod tests {
    use super::normalize_onelake_location;

    #[test]
    fn normalize_onelake_location_handles_each_form() {
        // Iceberg: workspace-relative namespace location -> abfss (the reported bug).
        assert_eq!(
            normalize_onelake_location("OneLake_LakeSail_Testing/LakeSail.Lakehouse/Tables/dbo")
                .as_deref(),
            Some("abfss://OneLake_LakeSail_Testing@onelake.dfs.fabric.microsoft.com/LakeSail.Lakehouse/Tables/dbo"),
        );
        // Delta/Unity: absolute https OneLake URL -> abfss (unchanged from before).
        assert_eq!(
            normalize_onelake_location(
                "https://onelake.dfs.fabric.microsoft.com/ws/item.Lakehouse/Tables/dbo/t",
            )
            .as_deref(),
            Some("abfss://ws@onelake.dfs.fabric.microsoft.com/item.Lakehouse/Tables/dbo/t"),
        );
        // Already absolute -> None so the caller keeps the original (never double-convert).
        assert_eq!(
            normalize_onelake_location("abfss://ws@onelake.dfs.fabric.microsoft.com/x"),
            None,
        );
        assert_eq!(normalize_onelake_location("s3://bucket/x"), None);
        // Single-slash scheme: `Url::parse` should detect it.
        assert_eq!(normalize_onelake_location("file:/tmp/onelake"), None);
        // Degenerate inputs -> no conversion.
        assert_eq!(normalize_onelake_location("single-segment"), None);
        assert_eq!(normalize_onelake_location("ws/"), None);
    }
}
