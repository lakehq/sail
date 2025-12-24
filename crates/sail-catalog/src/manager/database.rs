use std::sync::Arc;

use sail_common_datafusion::catalog::DatabaseStatus;

use crate::error::CatalogResult;
use crate::manager::CatalogManager;
use crate::provider::{CreateDatabaseOptions, DropDatabaseOptions, Namespace};
use crate::utils::{match_pattern, quote_names_if_needed};

impl CatalogManager {
    pub fn default_database(&self) -> CatalogResult<Namespace> {
        Ok(self.state()?.default_database.clone())
    }

    /// Sets the default database for the current session.
    /// An error is returned if the database does not exist in the default catalog.
    /// Note that even if this method succeeds, the default database may still become
    /// invalid if the database is dropped externally. In this case, subsequent calls
    /// that refer to the default database will return an error.
    pub async fn set_default_database<T: Into<Arc<str>>>(
        &self,
        database: Vec<T>,
    ) -> CatalogResult<()> {
        let database: Namespace = database.try_into()?;
        // Here we lock the state twice, first to get the default catalog provider,
        // and then to set the default database.
        // We do not hold the lock while validating the existence of the default database
        // via an async method call to the catalog provider.
        // This logic is not atomic, but it is acceptable since the database may be
        // modified externally anyway.
        // (We do not use the Tokio mutex since the ability to hold the lock
        // across await points is mostly not needed for the catalog manager.)
        //
        // We could have set the default database without validating its existence,
        // since subsequent calls that refer to the default database will report the error
        // anyway if the database does not exist. But here we follow the Spark behavior
        // where a non-existing default database is detected immediately.
        let provider = {
            let state = self.state()?;
            let default_catalog = state.default_catalog.clone();
            state.get_catalog(&default_catalog)?
        };
        let _ = provider.get_database(&database).await?;
        self.state()?.default_database = database;
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
        // TODO: Add options["owner"] = current user name
        //  https://github.com/apache/spark/blob/3d18fe1927f140b7f2429c7b88e5156a6e9155d7/core/src/main/scala/org/apache/spark/util/Utils.scala#L2429-L2443
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
