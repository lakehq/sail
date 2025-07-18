use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::{
    CatalogProvider, CreateNamespaceOptions, CreateTableOptions, CreateViewOptions,
    DeleteNamespaceOptions, DeleteTableOptions, DeleteViewOptions, Namespace, NamespaceStatus,
    TableKind, TableStatus,
};

struct MemoryNamespace {
    comment: Option<String>,
    location: Option<String>,
    properties: HashMap<String, String>,
    tables: HashMap<String, TableStatus>,
    views: HashMap<String, TableStatus>,
}

pub struct MemoryCatalogProvider {
    name: String,
    namespaces: Arc<Mutex<HashMap<Namespace, MemoryNamespace>>>,
}

impl MemoryCatalogProvider {
    pub fn new(name: String, database: Namespace) -> Self {
        let mut namespaces = HashMap::new();
        namespaces.insert(
            database,
            MemoryNamespace {
                comment: None,
                location: None,
                properties: HashMap::new(),
                tables: HashMap::new(),
                views: HashMap::new(),
            },
        );
        Self {
            name,
            namespaces: Arc::new(Mutex::new(namespaces)),
        }
    }

    fn namespaces(&self) -> CatalogResult<MutexGuard<'_, HashMap<Namespace, MemoryNamespace>>> {
        self.namespaces
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_namespace(
        &self,
        namespace: &Namespace,
        options: CreateNamespaceOptions,
    ) -> CatalogResult<()> {
        let CreateNamespaceOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;
        let mut namespaces = self.namespaces()?;
        if namespaces.contains_key(namespace) {
            if if_not_exists {
                Ok(())
            } else {
                Err(CatalogError::AlreadyExists(
                    "namespace",
                    namespace.to_string(),
                ))
            }
        } else {
            namespaces.insert(
                namespace.clone(),
                MemoryNamespace {
                    comment,
                    location,
                    properties: properties.into_iter().collect(),
                    tables: HashMap::new(),
                    views: HashMap::new(),
                },
            );
            Ok(())
        }
    }

    async fn delete_namespace(
        &self,
        namespace: &Namespace,
        options: DeleteNamespaceOptions,
    ) -> CatalogResult<()> {
        let DeleteNamespaceOptions {
            if_exists,
            cascade: _,
        } = options;
        let mut namespaces = self.namespaces()?;
        if namespaces.remove(namespace).is_none() {
            if if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("namespace", namespace.to_string()))
            }
        } else {
            Ok(())
        }
    }

    async fn get_namespace(&self, namespace: &Namespace) -> CatalogResult<NamespaceStatus> {
        let namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get(namespace) {
            Ok(NamespaceStatus {
                catalog: self.name.clone(),
                namespace: namespace.clone().into(),
                comment: ns.comment.clone(),
                location: ns.location.clone(),
                properties: ns.properties.clone(),
            })
        } else {
            Err(CatalogError::NotFound("namespace", namespace.to_string()))
        }
    }

    async fn list_namespaces(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<NamespaceStatus>> {
        let namespaces = self.namespaces()?;
        Ok(namespaces
            .iter()
            .filter(|(namespace, _)| {
                if let Some(prefix) = prefix {
                    prefix.is_parent_of(namespace)
                } else {
                    namespace.tail.is_empty()
                }
            })
            .map(|(namespace, status)| NamespaceStatus {
                catalog: self.name.clone(),
                namespace: namespace.clone().into(),
                comment: status.comment.clone(),
                location: status.location.clone(),
                properties: status.properties.clone(),
            })
            .collect())
    }

    async fn create_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<()> {
        let CreateTableOptions {
            schema,
            file_format,
            if_not_exists,
            or_replace,
            comment,
            location,
            column_defaults: _,
            constraints: _,
            table_partition_cols: _,
            file_sort_order: _,
            definition: _,
            properties,
        } = options;
        let mut namespaces = self.namespaces()?;
        let ns = namespaces
            .get_mut(namespace)
            .ok_or_else(|| CatalogError::NotFound("namespace", namespace.to_string()))?;
        if ns.tables.contains_key(table) {
            if if_not_exists {
                return Ok(());
            } else if or_replace {
                ns.tables.remove(table);
            } else {
                return Err(CatalogError::AlreadyExists("table", table.to_string()));
            }
        }
        let status = TableStatus {
            name: table.to_string(),
            kind: TableKind::Table {
                catalog: self.name.clone(),
                namespace: namespace.clone().into(),
                schema,
                format: file_format,
                comment,
                location,
                properties: properties.into_iter().collect(),
            },
        };
        ns.tables.insert(table.to_string(), status);
        Ok(())
    }

    async fn get_table(&self, namespace: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get(namespace) {
            if let Some(status) = ns.tables.get(table) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("table", table.to_string()))
    }

    async fn list_tables(&self, namespace: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get(namespace) {
            Ok(ns.tables.values().cloned().collect())
        } else {
            Err(CatalogError::NotFound("namespace", namespace.to_string()))
        }
    }

    async fn delete_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: DeleteTableOptions,
    ) -> CatalogResult<()> {
        let DeleteTableOptions {
            if_exists,
            purge: _,
        } = options;
        let mut namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get_mut(namespace) {
            if ns.tables.remove(table).is_some() || if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("table", table.to_string()))
            }
        } else if if_exists {
            Ok(())
        } else {
            Err(CatalogError::NotFound("namespace", namespace.to_string()))
        }
    }

    async fn create_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<()> {
        let CreateViewOptions {
            definition,
            schema,
            replace,
            comment,
            properties,
        } = options;
        let mut namespaces = self.namespaces()?;
        let ns = namespaces
            .get_mut(namespace)
            .ok_or_else(|| CatalogError::NotFound("namespace", namespace.to_string()))?;
        if ns.views.contains_key(view) {
            if replace {
                ns.views.remove(view);
            } else {
                return Err(CatalogError::AlreadyExists("view", view.to_string()));
            }
        }
        let status = TableStatus {
            name: view.to_string(),
            kind: TableKind::View {
                catalog: self.name.clone(),
                namespace: namespace.clone().into(),
                schema,
                definition,
                comment,
                properties: properties.into_iter().collect(),
            },
        };
        ns.views.insert(view.to_string(), status);
        Ok(())
    }

    async fn get_view(&self, namespace: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        let namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get(namespace) {
            if let Some(status) = ns.views.get(view) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound("view", view.to_string()))
    }

    async fn list_views(&self, namespace: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get(namespace) {
            Ok(ns.views.values().cloned().collect())
        } else {
            Err(CatalogError::NotFound("namespace", namespace.to_string()))
        }
    }

    async fn delete_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: DeleteViewOptions,
    ) -> CatalogResult<()> {
        let DeleteViewOptions { if_exists } = options;
        let mut namespaces = self.namespaces()?;
        if let Some(ns) = namespaces.get_mut(namespace) {
            if ns.views.remove(view).is_some() || if_exists {
                Ok(())
            } else {
                Err(CatalogError::NotFound("view", view.to_string()))
            }
        } else if if_exists {
            Ok(())
        } else {
            Err(CatalogError::NotFound("namespace", namespace.to_string()))
        }
    }
}
