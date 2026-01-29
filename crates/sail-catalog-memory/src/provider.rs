use std::collections::HashMap;

use dashmap::{DashMap, Entry};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions,
    DropViewOptions, Namespace,
};
use sail_common_datafusion::catalog::{DatabaseStatus, TableColumnStatus, TableKind, TableStatus};

struct MemoryDatabase {
    status: DatabaseStatus,
    tables: HashMap<String, TableStatus>,
    views: HashMap<String, TableStatus>,
}

/// An in-memory catalog provider.
pub struct MemoryCatalogProvider {
    name: String,
    databases: DashMap<Namespace, MemoryDatabase>,
}

impl MemoryCatalogProvider {
    pub fn new(
        name: String,
        initial_database: Namespace,
        initial_database_comment: Option<String>,
    ) -> Self {
        let databases = DashMap::new();
        databases.insert(
            initial_database.clone(),
            MemoryDatabase {
                status: DatabaseStatus {
                    catalog: name.clone(),
                    database: initial_database.into(),
                    comment: initial_database_comment,
                    location: None,
                    properties: vec![],
                },
                tables: HashMap::new(),
                views: HashMap::new(),
            },
        );
        Self { name, databases }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;
        let entry = self.databases.entry(database.clone());
        match entry {
            Entry::Occupied(entry) => {
                if if_not_exists {
                    Ok(entry.get().status.clone())
                } else {
                    Err(CatalogError::AlreadyExists(
                        "database",
                        database.to_string(),
                    ))
                }
            }
            Entry::Vacant(entry) => {
                let status = DatabaseStatus {
                    catalog: self.name.clone(),
                    database: database.clone().into(),
                    comment,
                    location,
                    properties,
                };
                let db = MemoryDatabase {
                    status: status.clone(),
                    tables: HashMap::new(),
                    views: HashMap::new(),
                };
                entry.insert(db);
                Ok(status)
            }
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        if let Some(db) = self.databases.get(database) {
            Ok(db.status.clone())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        Ok(self
            .databases
            .iter()
            .filter(|item| {
                if let Some(prefix) = prefix {
                    prefix.is_parent_of(item.key())
                } else {
                    item.key().tail.is_empty()
                }
            })
            .map(|item| item.value().status.clone())
            .collect())
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let DropDatabaseOptions {
            if_exists,
            cascade: _,
        } = options;
        if self.databases.remove(database).is_none() {
            if if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("database", database.to_string()))
            }
        } else {
            Ok(())
        }
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let CreateTableOptions {
            columns,
            comment,
            constraints,
            location,
            format,
            partition_by,
            sort_by,
            bucket_by,
            if_not_exists,
            replace,
            options,
            properties,
        } = options;
        if partition_by.iter().any(|f| f.transform.is_some()) {
            return Err(CatalogError::NotSupported(
                "partition transforms are not supported by memory catalog".to_string(),
            ));
        }
        let mut db = self
            .databases
            .get_mut(database)
            .ok_or_else(|| CatalogError::NotFound("database", database.to_string()))?;
        if let Some(status) = db.tables.get(table) {
            if if_not_exists {
                return Ok(status.clone());
            } else if replace {
                db.tables.remove(table);
            } else {
                return Err(CatalogError::AlreadyExists("table", table.to_string()));
            }
        }
        let columns = columns
            .into_iter()
            .map(|x| {
                let CreateTableColumnOptions {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default,
                    generated_always_as,
                } = x;
                let is_partition = partition_by
                    .iter()
                    .any(|x| x.column.eq_ignore_ascii_case(&name));
                let is_bucket = bucket_by
                    .as_ref()
                    .is_some_and(|b| b.columns.iter().any(|x| x.eq_ignore_ascii_case(&name)));
                TableColumnStatus {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default,
                    generated_always_as,
                    is_partition,
                    is_bucket,
                    is_cluster: false,
                }
            })
            .collect();
        let status = TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: table.to_string(),
            kind: TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by: partition_by.into_iter().map(|f| f.column).collect(),
                sort_by,
                bucket_by,
                options,
                properties,
            },
        };
        db.tables.insert(table.to_string(), status.clone());
        Ok(status)
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        if let Some(db) = self.databases.get(database) {
            if let Some(status) = db.tables.get(table) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("table", table.to_string()))
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(db) = self.databases.get(database) {
            Ok(db.tables.values().cloned().collect())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        // In Spark, the `DROP TABLE ... PURGE` SQL statement deletes data if the table
        // is managed by the Hive metastore. `PURGE` is ignored if the table is external.
        // In Sail, all tables are external, so we ignore the `purge` option.
        let DropTableOptions {
            if_exists,
            purge: _,
        } = options;
        if let Some(mut db) = self.databases.get_mut(database) {
            if db.tables.remove(table).is_some() || if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("table", table.to_string()))
            }
        } else if if_exists {
            Ok(())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties,
        } = options;
        let mut db = self
            .databases
            .get_mut(database)
            .ok_or_else(|| CatalogError::NotFound("database", database.to_string()))?;
        if let Some(status) = db.views.get(view) {
            if if_not_exists {
                return Ok(status.clone());
            } else if replace {
                db.views.remove(view);
            } else {
                return Err(CatalogError::AlreadyExists("view", view.to_string()));
            }
        }
        let columns = columns
            .into_iter()
            .map(|x| {
                let CreateViewColumnOptions {
                    name,
                    data_type,
                    nullable,
                    comment,
                } = x;
                TableColumnStatus {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }
            })
            .collect();
        let status = TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: view.to_string(),
            kind: TableKind::View {
                columns,
                definition,
                comment,
                properties: properties.into_iter().collect(),
            },
        };
        db.views.insert(view.to_string(), status.clone());
        Ok(status)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        if let Some(db) = self.databases.get(database) {
            if let Some(status) = db.views.get(view) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("view", view.to_string()))
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(db) = self.databases.get(database) {
            Ok(db.views.values().cloned().collect())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        let DropViewOptions { if_exists } = options;
        if let Some(mut db) = self.databases.get_mut(database) {
            if db.views.remove(view).is_some() || if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("view", view.to_string()))
            }
        } else if if_exists {
            Ok(())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }
}
