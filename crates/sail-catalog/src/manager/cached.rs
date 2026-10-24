use std::collections::{HashMap, HashSet};
use std::sync::MutexGuard;

use sail_common::spec::StorageLevel;
use sail_common_datafusion::catalog::TableStatus;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;

/// The tables and query plans marked as cached in the session.
///
/// This only keeps track of what has been cached, so that the cache status is reported
/// as in Spark. The data is not materialized, and queries always read the source data.
#[derive(Debug, Default)]
pub(super) struct CachedObjects {
    tables: HashSet<CachedTableKey>,
    plans: HashMap<Vec<u8>, StorageLevel>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CachedTableKey {
    catalog: Option<String>,
    database: Vec<String>,
    name: String,
}

impl From<&TableStatus> for CachedTableKey {
    fn from(status: &TableStatus) -> Self {
        Self {
            catalog: status.catalog.clone(),
            database: status.database.clone(),
            name: status.name.clone(),
        }
    }
}

impl CatalogManager {
    fn cached(&self) -> CatalogResult<MutexGuard<'_, CachedObjects>> {
        self.cached
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    /// Marks the table or view as cached.
    /// An error is returned if the table or view does not exist.
    pub async fn cache_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<()> {
        let status = self.get_table_or_view(table).await?;
        self.cached()?.tables.insert((&status).into());
        Ok(())
    }

    /// Returns whether the table or view is cached.
    /// An error is returned if the table or view does not exist.
    pub async fn is_table_cached<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<bool> {
        let status = self.get_table_or_view(table).await?;
        Ok(self.cached()?.tables.contains(&(&status).into()))
    }

    /// Removes the table or view from the cache.
    /// An error is returned if the table or view does not exist, unless `if_exists` is true.
    pub async fn uncache_table<T: AsRef<str>>(
        &self,
        table: &[T],
        if_exists: bool,
    ) -> CatalogResult<()> {
        let status = match self.get_table_or_view(table).await {
            Ok(status) => status,
            Err(CatalogError::NotFound(_, _)) if if_exists => return Ok(()),
            Err(e) => return Err(e),
        };
        self.cached()?.tables.remove(&(&status).into());
        Ok(())
    }

    /// Marks the query plan as cached with the storage level.
    /// The plan is identified by its serialized form.
    pub fn cache_plan(&self, plan: Vec<u8>, storage_level: StorageLevel) -> CatalogResult<()> {
        self.cached()?.plans.insert(plan, storage_level);
        Ok(())
    }

    /// Removes the query plan from the cache.
    pub fn uncache_plan(&self, plan: &[u8]) -> CatalogResult<()> {
        self.cached()?.plans.remove(plan);
        Ok(())
    }

    /// Returns the storage level of the query plan if it is cached.
    pub fn get_cached_plan_storage_level(
        &self,
        plan: &[u8],
    ) -> CatalogResult<Option<StorageLevel>> {
        Ok(self.cached()?.plans.get(plan).cloned())
    }

    /// Removes all tables, views, and query plans from the cache.
    pub fn clear_cache(&self) -> CatalogResult<()> {
        let mut cached = self.cached()?;
        cached.tables.clear();
        cached.plans.clear();
        Ok(())
    }
}
