mod cache;
mod namespace;
mod options;
mod runtime;

pub use cache::*;
pub use namespace::*;
pub use options::*;
pub use runtime::*;
use sail_common_datafusion::catalog::{DatabaseStatus, TableStatus};

use crate::error::{CatalogError, CatalogResult};
use crate::lakehouse::{
    BeginTableAccessRequest, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest, LakehouseCreatePlan,
    LakehouseCreateRequest, LakehouseResolvedTable, LakehouseScanPlanningRequest,
    LakehouseScanPlanningResponse, ResolveLakehouseTableRequest, TableAccessSession,
};

/// A trait that defines the interface for a catalog.
/// A catalog contains *databases*, where each database has a multi-level name
/// that represents a *namespace*.
/// A database contains *objects* such as *tables* and *views*.
#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    /// The name of the catalog in the session.
    /// Note that the same catalog can be registered under different names
    /// in different sessions.
    fn get_name(&self) -> &str;

    /// Creates a new database in the catalog.
    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus>;

    /// Gets the status of a database in the catalog.
    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus>;

    /// Lists all databases in the catalog.
    /// If `prefix` is provided, only databases whose namespace starts with the prefix
    /// are returned.
    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>>;

    /// Drops a database in the catalog.
    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()>;

    /// Creates a table in the catalog.
    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus>;

    /// Whether catalog `CREATE TABLE` needs the table format to create storage metadata before
    /// registering the catalog object. Providers that can reject create options should do so here
    /// before storage metadata is materialized.
    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        let _ = options;
        Ok(CreateTableMetadataRequirement::None)
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        Vec::new()
    }

    async fn resolve_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: ResolveLakehouseTableRequest,
    ) -> CatalogResult<LakehouseResolvedTable> {
        let _ = (database, table, request);
        Err(CatalogError::UnsupportedCapability(
            "lakehouse table resolution".to_string(),
        ))
    }

    async fn plan_lakehouse_create(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCreateRequest,
    ) -> CatalogResult<LakehouseCreatePlan> {
        let _ = (database, table, request);
        Err(CatalogError::UnsupportedCapability(
            "lakehouse CREATE TABLE planning".to_string(),
        ))
    }

    async fn begin_table_access(
        &self,
        database: &Namespace,
        table: &str,
        request: BeginTableAccessRequest,
    ) -> CatalogResult<TableAccessSession> {
        let _ = (database, table, request);
        Err(CatalogError::UnsupportedCapability(
            "table access sessions".to_string(),
        ))
    }

    async fn plan_lakehouse_scan(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseScanPlanningRequest,
    ) -> CatalogResult<LakehouseScanPlanningResponse> {
        let _ = (database, table, request);
        Err(CatalogError::UnsupportedCapability(
            "lakehouse scan planning".to_string(),
        ))
    }

    async fn commit_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        let LakehouseCommitRequest {
            context,
            format,
            requirements,
            updates,
            payload,
        } = request;
        let _status = self
            .commit_table(
                database,
                table,
                CommitTableOptions {
                    format,
                    lakehouse_table: Some(context.clone()),
                    requirements,
                    updates,
                },
            )
            .await?;
        Ok(LakehouseCommitOutcome::Committed { context, payload })
    }

    async fn get_delta_ratified_commits(
        &self,
        database: &Namespace,
        table: &str,
        request: DeltaRatifiedCommitRequest,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        let DeltaRatifiedCommitRequest {
            context,
            table_uri,
            start_version,
            end_version,
        } = request;
        let response = self
            .get_table_commits(
                database,
                table,
                GetTableCommitsOptions {
                    format: "delta".to_string(),
                    lakehouse_table: Some(context),
                    table_uri,
                    start_version,
                    end_version,
                },
            )
            .await?;
        Ok(response.into())
    }

    /// Gets the status of a table in the catalog.
    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus>;

    /// Lists all tables in a database in the catalog.
    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    /// Drops a table in the catalog.
    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()>;

    /// Alters a table in the catalog.
    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()>;

    /// Commits table-format metadata through the catalog control plane.
    async fn commit_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CommitTableOptions,
    ) -> CatalogResult<TableStatus> {
        let _ = (database, table);
        Err(CatalogError::NotSupported(format!(
            "catalog commit for {} tables",
            options.format
        )))
    }

    /// Gets table-format commits tracked by the catalog control plane.
    async fn get_table_commits(
        &self,
        database: &Namespace,
        table: &str,
        options: GetTableCommitsOptions,
    ) -> CatalogResult<GetTableCommitsResponse> {
        let _ = (database, table);
        Err(crate::error::CatalogError::NotSupported(format!(
            "catalog commit discovery for {} tables",
            options.format
        )))
    }

    /// Creates a view in the catalog.
    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus>;

    /// Gets the status of a view in the catalog.
    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus>;

    /// Lists all views in a database in the catalog.
    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    /// Drops a view in the catalog.
    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()>;
}
