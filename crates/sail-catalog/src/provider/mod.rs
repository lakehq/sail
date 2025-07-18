mod namespace;
mod options;
mod status;

pub use namespace::*;
pub use options::*;
pub use status::*;

use crate::error::CatalogResult;

#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    fn get_name(&self) -> &str;

    async fn create_namespace(
        &self,
        namespace: &Namespace,
        options: CreateNamespaceOptions,
    ) -> CatalogResult<()>;

    async fn delete_namespace(
        &self,
        namespace: &Namespace,
        options: DeleteNamespaceOptions,
    ) -> CatalogResult<()>;

    async fn get_namespace(&self, namespace: &Namespace) -> CatalogResult<NamespaceStatus>;

    async fn list_namespaces(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<NamespaceStatus>>;

    async fn create_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<()>;

    async fn get_table(&self, namespace: &Namespace, table: &str) -> CatalogResult<TableStatus>;

    async fn list_tables(&self, namespace: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn delete_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: DeleteTableOptions,
    ) -> CatalogResult<()>;

    async fn create_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<()>;

    async fn get_view(&self, namespace: &Namespace, view: &str) -> CatalogResult<TableStatus>;

    async fn list_views(&self, namespace: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn delete_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: DeleteViewOptions,
    ) -> CatalogResult<()>;
}
