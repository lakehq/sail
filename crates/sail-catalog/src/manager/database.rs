use std::sync::Arc;

use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::{
    CreateNamespaceOptions, DeleteNamespaceOptions, Namespace, NamespaceMetadata,
};
use crate::utils::{match_pattern, quote_names_if_needed};

impl CatalogManager {
    pub fn default_database(&self) -> CatalogResult<Namespace> {
        Ok(self.state()?.default_database.clone())
    }

    pub fn set_default_database<T: Into<Arc<str>>>(&self, database: Vec<T>) -> CatalogResult<()> {
        self.state()?.default_database = database.try_into()?;
        Ok(())
    }

    pub async fn get_database<T: AsRef<str>>(
        &self,
        database: &[T],
    ) -> CatalogResult<NamespaceMetadata> {
        let (catalog, namespace) = self.resolve_database_reference(database)?;
        let provider = self.get_catalog(&catalog)?;
        provider.get_namespace(&namespace).await
    }

    pub async fn list_databases<T: AsRef<str>>(
        &self,
        qualifier: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<NamespaceMetadata>> {
        let (catalog, prefix) = self.resolve_optional_database_reference(qualifier)?;
        let provider = self.get_catalog(&catalog)?;
        Ok(provider
            .list_namespaces(prefix.as_ref())
            .await?
            .into_iter()
            .filter(|x| match_pattern(quote_names_if_needed(&x.namespace).as_str(), pattern))
            .collect())
    }

    pub async fn create_database<T: AsRef<str>>(
        &self,
        database: &[T],
        options: CreateNamespaceOptions,
    ) -> CatalogResult<()> {
        let (catalog, namespace) = self.resolve_database_reference(database)?;
        let provider = self.get_catalog(&catalog)?;
        provider.create_namespace(&namespace, options).await
    }

    pub async fn delete_database<T: AsRef<str>>(
        &self,
        database: &[T],
        options: DeleteNamespaceOptions,
    ) -> CatalogResult<()> {
        let (catalog, namespace) = self.resolve_database_reference(database)?;
        let provider = self.get_catalog(&catalog)?;
        provider.delete_namespace(&namespace, options).await
    }
}
