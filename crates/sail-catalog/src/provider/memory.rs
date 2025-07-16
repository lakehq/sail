use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::CatalogResult;
use crate::provider::common::{CatalogProvider, Namespace};
use crate::provider::{
    CreateNamespaceOptions, CreateTableOptions, CreateViewOptions, DeleteNamespaceOptions,
    DeleteTableOptions, DeleteViewOptions, NamespaceMetadata, TableMetadata,
};

struct CatalogTable {}

#[allow(unused)]
struct CatalogNamespace {
    metadata: HashMap<String, String>,
    tables: HashMap<String, CatalogTable>,
}

impl CatalogNamespace {
    fn new() -> Self {
        Self {
            metadata: HashMap::new(),
            tables: HashMap::new(),
        }
    }
}

#[allow(unused)]
pub struct MemoryCatalogProvider {
    namespaces: Arc<Mutex<HashMap<Namespace, CatalogNamespace>>>,
}

impl MemoryCatalogProvider {
    pub fn new(initial_database: Namespace) -> Self {
        let mut namespaces = HashMap::new();
        namespaces.insert(initial_database, CatalogNamespace::new());
        Self {
            namespaces: Arc::new(Mutex::new(namespaces)),
        }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    async fn create_namespace(
        &self,
        _namespace: &Namespace,
        _options: CreateNamespaceOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn delete_namespace(
        &self,
        _namespace: &Namespace,
        _options: DeleteNamespaceOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn get_namespace(&self, _namespace: &Namespace) -> CatalogResult<NamespaceMetadata> {
        todo!()
    }

    async fn list_namespaces(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<NamespaceMetadata>> {
        todo!()
    }

    async fn create_table(
        &self,
        _namespace: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn get_table(
        &self,
        _namespace: &Namespace,
        _table: &str,
    ) -> CatalogResult<TableMetadata> {
        todo!()
    }

    async fn list_tables(&self, _namespace: &Namespace) -> CatalogResult<Vec<TableMetadata>> {
        todo!()
    }

    async fn delete_table(
        &self,
        _namespace: &Namespace,
        _table: &str,
        _options: DeleteTableOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn create_view(
        &self,
        _namespace: &Namespace,
        _view: &str,
        _options: CreateViewOptions,
    ) -> CatalogResult<()> {
        todo!()
    }

    async fn get_view(&self, _namespace: &Namespace, _view: &str) -> CatalogResult<TableMetadata> {
        todo!()
    }

    async fn list_views(&self, _namespace: &Namespace) -> CatalogResult<Vec<TableMetadata>> {
        todo!()
    }

    async fn delete_view(
        &self,
        _namespace: &Namespace,
        _view: &str,
        _options: DeleteViewOptions,
    ) -> CatalogResult<()> {
        todo!()
    }
}
