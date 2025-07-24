use std::sync::Arc;

use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::{CreateDatabaseOptions, DatabaseStatus, DropDatabaseOptions, Namespace};
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
    ) -> CatalogResult<DatabaseStatus> {
        let (provider, database) = self.resolve_database(database)?;
        provider.get_database(&database).await
    }

    pub async fn list_databases<T: AsRef<str>>(
        &self,
        qualifier: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let (provider, prefix) = self.resolve_optional_database(qualifier)?;
        Ok(provider
            .list_databases(prefix.as_ref())
            .await?
            .into_iter()
            .filter(|x| match_pattern(quote_names_if_needed(&x.database).as_str(), pattern))
            .collect())
    }

    pub async fn create_database<T: AsRef<str>>(
        &self,
        database: &[T],
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let (provider, database) = self.resolve_database(database)?;
        provider.create_database(&database, options).await
    }

    pub async fn drop_database<T: AsRef<str>>(
        &self,
        database: &[T],
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let (provider, database) = self.resolve_database(database)?;
        provider.drop_database(&database, options).await
    }
}
