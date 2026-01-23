use std::sync::Arc;

use datafusion::common::{DFSchema, TableReference};
use datafusion::logical_expr::{LogicalPlan, TableScan};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
    DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
};
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};
use sail_common_datafusion::system::catalog::{SystemCatalog, SystemDatabase, SystemTable};

use crate::table_source::SystemTableSource;

pub const SYSTEM_CATALOG_NAME: &str = "system";

pub struct SystemCatalogProvider;

impl SystemCatalogProvider {
    fn get_database_status(
        database: &Namespace,
        db: &SystemDatabase,
    ) -> CatalogResult<DatabaseStatus> {
        Ok(DatabaseStatus {
            catalog: SYSTEM_CATALOG_NAME.to_string(),
            database: database.clone().into(),
            comment: Some(db.description().to_string()),
            location: None,
            properties: vec![],
        })
    }

    fn get_table_status(
        database: &Namespace,
        table: &str,
        t: SystemTable,
    ) -> CatalogResult<TableStatus> {
        let columns = t
            .columns()
            .iter()
            .map(|col| TableColumnStatus {
                name: col.name.to_string(),
                data_type: col.arrow_type.clone(),
                nullable: col.nullable,
                comment: Some(col.description.to_string()),
                default: None,
                generated_always_as: None,
                is_partition: false,
                is_bucket: false,
                is_cluster: false,
            })
            .collect();
        let status = TableStatus {
            catalog: Some(SYSTEM_CATALOG_NAME.to_string()),
            database: database.clone().into(),
            name: table.to_string(),
            kind: TableKind::TemporaryView {
                plan: Arc::new(LogicalPlan::TableScan(TableScan {
                    table_name: TableReference::Full {
                        catalog: Arc::from(SYSTEM_CATALOG_NAME),
                        // The tail of the namespace is always empty for system databases.
                        schema: database.head.clone(),
                        table: Arc::from(t.name()),
                    },
                    source: Arc::new(SystemTableSource::new(t)),
                    projection: None,
                    projected_schema: Arc::new(DFSchema::try_from(t.schema())?),
                    filters: vec![],
                    fetch: None,
                })),
                columns,
                comment: Some(t.description().to_string()),
                properties: vec![],
            },
        };
        Ok(status)
    }
}

#[async_trait::async_trait]
impl CatalogProvider for SystemCatalogProvider {
    fn get_name(&self) -> &str {
        SYSTEM_CATALOG_NAME
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported(
            "create database in system catalog".to_string(),
        ))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let Namespace { head, tail } = database;
        if tail.is_empty() {
            if let Some(db) = SystemDatabase::get(head) {
                return Self::get_database_status(database, &db);
            }
        }
        Err(CatalogError::NotFound("database", database.to_string()))
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let mut result = vec![];
        for db in SystemCatalog::databases() {
            let database = Namespace {
                head: Arc::from(db.name()),
                tail: vec![],
            };
            if prefix.is_none_or(|p| p.starts_with(&database)) {
                let status = Self::get_database_status(&database, db)?;
                result.push(status);
            }
        }
        Ok(result)
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "drop database in system catalog".to_string(),
        ))
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "create table in system catalog".to_string(),
        ))
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let Namespace { head, tail } = database;
        if tail.is_empty() {
            if let Some(db) = SystemDatabase::get(head) {
                for t in db.tables() {
                    if table.eq_ignore_ascii_case(t.name()) {
                        return Self::get_table_status(database, table, *t);
                    }
                }
                return Err(CatalogError::NotFound("table", table.to_string()));
            }
        }
        Err(CatalogError::NotFound("database", database.to_string()))
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let Namespace { head, tail } = database;
        if tail.is_empty() {
            if let Some(db) = SystemDatabase::get(head) {
                let mut result = vec![];
                for t in db.tables() {
                    let status = Self::get_table_status(database, t.name(), *t)?;
                    result.push(status);
                }
                return Ok(result);
            }
        }
        Err(CatalogError::NotFound("database", database.to_string()))
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "drop table in system catalog".to_string(),
        ))
    }

    async fn create_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported(
            "create view in system catalog".to_string(),
        ))
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let Namespace { head, tail } = database;
        if tail.is_empty() && SystemDatabase::get(head).is_some() {
            return Err(CatalogError::NotFound("view", view.to_string()));
        }
        Err(CatalogError::NotFound("database", database.to_string()))
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let Namespace { head, tail } = database;
        if tail.is_empty() && SystemDatabase::get(head).is_some() {
            return Ok(vec![]);
        }
        Err(CatalogError::NotFound("database", database.to_string()))
    }

    async fn drop_view(
        &self,
        _database: &Namespace,
        _view: &str,
        _options: DropViewOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported(
            "drop view in system catalog".to_string(),
        ))
    }
}
