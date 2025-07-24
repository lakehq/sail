use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions, CreateTableOptions,
    CreateViewColumnOptions, CreateViewOptions, DatabaseStatus, DropDatabaseOptions,
    DropTableOptions, DropViewOptions, Namespace, TableColumnStatus, TableKind, TableStatus,
};

struct MemoryDatabase {
    status: DatabaseStatus,
    tables: HashMap<String, TableStatus>,
    views: HashMap<String, TableStatus>,
}

pub struct MemoryCatalogProvider {
    name: String,
    databases: Arc<Mutex<HashMap<Namespace, MemoryDatabase>>>,
}

impl MemoryCatalogProvider {
    pub fn new(name: String, database: Namespace) -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            database.clone(),
            MemoryDatabase {
                status: DatabaseStatus {
                    catalog: name.clone(),
                    database: database.into(),
                    comment: None,
                    location: None,
                    properties: vec![],
                },
                tables: HashMap::new(),
                views: HashMap::new(),
            },
        );
        Self {
            name,
            databases: Arc::new(Mutex::new(databases)),
        }
    }

    fn databases(&self) -> CatalogResult<MutexGuard<'_, HashMap<Namespace, MemoryDatabase>>> {
        self.databases
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
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
        let mut databases = self.databases()?;
        if let Some(db) = databases.get(database) {
            if if_not_exists {
                Ok(db.status.clone())
            } else {
                Err(CatalogError::AlreadyExists(
                    "database",
                    database.to_string(),
                ))
            }
        } else {
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
            databases.insert(database.clone(), db);
            Ok(status)
        }
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
        let mut databases = self.databases()?;
        if databases.remove(database).is_none() {
            if if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("database", database.to_string()))
            }
        } else {
            Ok(())
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let databases = self.databases()?;
        if let Some(db) = databases.get(database) {
            Ok(db.status.clone())
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let databases = self.databases()?;
        Ok(databases
            .iter()
            .filter(|(database, _)| {
                if let Some(prefix) = prefix {
                    prefix.is_parent_of(database)
                } else {
                    database.tail.is_empty()
                }
            })
            .map(|(_, db)| db.status.clone())
            .collect())
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
        let mut databases = self.databases()?;
        let db = databases
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
                let is_partition = partition_by.iter().any(|x| x.eq_ignore_ascii_case(&name));
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
            name: table.to_string(),
            kind: TableKind::Table {
                catalog: self.name.clone(),
                database: database.clone().into(),
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
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
        let databases = self.databases()?;
        if let Some(db) = databases.get(database) {
            if let Some(status) = db.tables.get(table) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("table", table.to_string()))
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let databases = self.databases()?;
        if let Some(db) = databases.get(database) {
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
        let DropTableOptions {
            if_exists,
            purge: _,
        } = options;
        let mut databases = self.databases()?;
        if let Some(db) = databases.get_mut(database) {
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
        let mut databases = self.databases()?;
        let db = databases
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
            name: view.to_string(),
            kind: TableKind::View {
                catalog: self.name.clone(),
                database: database.clone().into(),
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
        let databases = self.databases()?;
        if let Some(db) = databases.get(database) {
            if let Some(status) = db.views.get(view) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("view", view.to_string()))
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let databases = self.databases()?;
        if let Some(db) = databases.get(database) {
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
        let mut databases = self.databases()?;
        if let Some(db) = databases.get_mut(database) {
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
