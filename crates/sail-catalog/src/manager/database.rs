use std::sync::Arc;

use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::{CreateNamespaceOptions, DeleteNamespaceOptions, Namespace, NamespaceStatus};
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
    ) -> CatalogResult<NamespaceStatus> {
        let (provider, namespace) = self.resolve_database(database)?;
        provider.get_namespace(&namespace).await
    }

    pub async fn list_databases<T: AsRef<str>>(
        &self,
        qualifier: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<NamespaceStatus>> {
        let (provider, prefix) = self.resolve_optional_database(qualifier)?;
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
        let (provider, namespace) = self.resolve_database(database)?;
        provider.create_namespace(&namespace, options).await
    }

    pub async fn delete_database<T: AsRef<str>>(
        &self,
        database: &[T],
        options: DeleteNamespaceOptions,
    ) -> CatalogResult<()> {
        let (provider, namespace) = self.resolve_database(database)?;
        provider.delete_namespace(&namespace, options).await
    }
}
