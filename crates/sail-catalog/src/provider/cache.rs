use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use moka::Expiry;
use moka::future::{Cache, CacheBuilder};
use moka::policy::EvictionPolicy;
use sail_common::config::CatalogCacheConfig;
use sail_common_datafusion::catalog::{DatabaseStatus, LakehouseOperation, TableStatus};

use crate::error::{CatalogError, CatalogResult};
use crate::lakehouse::{
    BeginTableAccessRequest, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
    LakehouseCapability, LakehouseCommitOutcome, LakehouseCommitRequest, LakehouseCreatePlan,
    LakehouseCreateRequest, LakehouseResolvedTable, LakehouseScanPlanningRequest,
    LakehouseScanPlanningResponse, ResolveLakehouseTableRequest, TableAccessPurpose,
    TableAccessSession,
};
use crate::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableMetadataRequirement,
    CreateTableOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions,
    Namespace,
};

#[derive(Clone)]
pub struct CatalogCacheBundle {
    pub database_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    pub table_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    pub view_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    pub loaded_table_cache: Option<LoadedTableCache>,
}

type TableKey = (Namespace, String);

/// A cached table access session is dropped this long before its credentials expire,
/// so it is never handed out with credentials about to lapse.
const TABLE_ACCESS_EXPIRY_SKEW_MS: i64 = 60_000;

/// The time-to-live of a loaded table when the table cache has no TTL.
/// A loaded table pins a snapshot of the table, so unlike a listing it always expires.
const DEFAULT_LOADED_TABLE_TTL_SECS: u64 = 60;

/// Caches what loading a single table returns: the table status, and the resolved
/// lakehouse table and the table access session for reads. It follows the table
/// cache settings, so a remote catalog is not asked to load the same table again
/// for every statement that references it.
///
/// Only reads are served from the cache. Resolving a table for any other operation
/// drops the cached entries for that table first, so writes always plan against the
/// current table metadata. Commits, `ALTER TABLE`, `DROP TABLE`, and `CREATE TABLE`
/// through Sail drop the cached entries for the table as well.
///
/// A load that was already in flight when an invalidation happened must not put its
/// result back, since it may predate the change. Every invalidation bumps a generation
/// counter, and a load only keeps its entry if the generation did not move while it ran.
#[derive(Clone)]
pub struct LoadedTableCache {
    status: Cache<TableKey, TableStatus>,
    resolved: Cache<(TableKey, ResolveLakehouseTableRequest), LakehouseResolvedTable>,
    access: Cache<(TableKey, BeginTableAccessRequest), TableAccessSession>,
    generation: Arc<AtomicU64>,
}

struct TableAccessExpiry;

impl Expiry<(TableKey, BeginTableAccessRequest), TableAccessSession> for TableAccessExpiry {
    fn expire_after_create(
        &self,
        _key: &(TableKey, BeginTableAccessRequest),
        value: &TableAccessSession,
        _created_at: Instant,
    ) -> Option<Duration> {
        value.expires_at_ms.map(table_access_remaining)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// How long a table access session whose credentials expire at `expires_at_ms`
/// may stay cached.
fn table_access_remaining(expires_at_ms: i64) -> Duration {
    let remaining = expires_at_ms
        .saturating_sub(TABLE_ACCESS_EXPIRY_SKEW_MS)
        .saturating_sub(now_ms());
    Duration::from_millis(u64::try_from(remaining).unwrap_or(0))
}

fn loaded_table_cache_builder<K, V>(
    size: Option<usize>,
    ttl_secs: Option<u64>,
) -> CacheBuilder<K, V, Cache<K, V>>
where
    K: std::hash::Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    let ttl = ttl_secs
        .filter(|&t| t > 0)
        .unwrap_or(DEFAULT_LOADED_TABLE_TTL_SECS);
    let mut builder = Cache::builder()
        .eviction_policy(EvictionPolicy::lru())
        .support_invalidation_closures()
        .time_to_live(Duration::from_secs(ttl));
    if let Some(size) = size.filter(|&s| s > 0) {
        builder = builder.max_capacity(size as u64);
    }
    builder
}

impl LoadedTableCache {
    pub fn new(size: Option<usize>, ttl_secs: Option<u64>) -> Self {
        Self {
            status: loaded_table_cache_builder(size, ttl_secs).build(),
            resolved: loaded_table_cache_builder(size, ttl_secs).build(),
            access: loaded_table_cache_builder(size, ttl_secs)
                .expire_after(TableAccessExpiry)
                .build(),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    fn key(database: &Namespace, table: &str) -> TableKey {
        (database.clone(), table.to_string())
    }

    /// The generation to pass to [`Self::insert_loaded`] for a load that starts now.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Caches a value loaded since `generation`, unless an invalidation happened meanwhile.
    /// The generation is checked again after the insert, so an invalidation that lands
    /// between the check and the insert still removes the entry.
    async fn insert_loaded<K, V>(&self, cache: &Cache<K, V>, key: K, value: V, generation: u64)
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        if self.generation() != generation {
            return;
        }
        cache.insert(key.clone(), value).await;
        if self.generation() != generation {
            cache.invalidate(&key).await;
        }
    }

    async fn invalidate_table(&self, database: &Namespace, table: &str) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let key = Self::key(database, table);
        self.status.invalidate(&key).await;
        self.invalidate_requests(move |k| *k == key);
    }

    fn invalidate_database(&self, database: &Namespace) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let status_database = database.clone();
        if let Err(e) = self
            .status
            .invalidate_entries_if(move |(ns, _), _| *ns == status_database)
        {
            log::warn!("failed to invalidate the loaded table cache: {e}");
        }
        let database = database.clone();
        self.invalidate_requests(move |(ns, _)| *ns == database);
    }

    /// Invalidates the cached resolved tables and table access sessions whose table matches.
    fn invalidate_requests<F>(&self, matches: F)
    where
        F: Fn(&TableKey) -> bool + Clone + Send + Sync + 'static,
    {
        let resolved_matches = matches.clone();
        let results = [
            self.resolved
                .invalidate_entries_if(move |(k, _), _| resolved_matches(k))
                .map(|_| ()),
            self.access
                .invalidate_entries_if(move |(k, _), _| matches(k))
                .map(|_| ()),
        ];
        for result in results {
            if let Err(e) = result {
                log::warn!("failed to invalidate the loaded table cache: {e}");
            }
        }
    }
}

pub struct CatalogCacheManager {
    caches: RwLock<HashMap<String, Arc<CatalogCacheBundle>>>,
}

impl Default for CatalogCacheManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogCacheManager {
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_cache(&self, name: &str) -> CatalogResult<Option<Arc<CatalogCacheBundle>>> {
        let caches = self
            .caches
            .read()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        Ok(caches.get(name).cloned())
    }

    pub fn set_cache(&self, name: String, bundle: Arc<CatalogCacheBundle>) -> CatalogResult<()> {
        let mut caches = self
            .caches
            .write()
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        caches.insert(name, bundle);
        Ok(())
    }
}

pub struct CachingCatalogProvider<P: CatalogProvider + ?Sized> {
    inner: Arc<P>,
    database_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    table_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    view_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    loaded_table_cache: Option<LoadedTableCache>,
}

impl<P: CatalogProvider + ?Sized> CachingCatalogProvider<P> {
    pub fn new(
        inner: Arc<P>,
        config: CatalogCacheConfig,
        global_bundle: Option<Arc<CatalogCacheBundle>>,
    ) -> Self {
        let database_cache = match config.database_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => global_bundle
                .as_ref()
                .and_then(|b| b.database_cache.clone()),
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.database_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.database_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        let table_cache = match config.table_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => {
                global_bundle.as_ref().and_then(|b| b.table_cache.clone())
            }
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.table_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.table_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        let loaded_table_cache = match config.table_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => global_bundle
                .as_ref()
                .and_then(|b| b.loaded_table_cache.clone()),
            sail_common::config::CacheType::Session => Some(LoadedTableCache::new(
                config.table_cache_size,
                config.table_cache_ttl_secs,
            )),
        };

        let view_cache = match config.view_cache_type {
            sail_common::config::CacheType::None => None,
            sail_common::config::CacheType::Global => {
                global_bundle.as_ref().and_then(|b| b.view_cache.clone())
            }
            sail_common::config::CacheType::Session => {
                let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
                if let Some(size) = config.view_cache_size.filter(|&s| s > 0) {
                    builder = builder.max_capacity(size as u64);
                }
                if let Some(ttl) = config.view_cache_ttl_secs.filter(|&t| t > 0) {
                    builder = builder.time_to_live(Duration::from_secs(ttl));
                }
                Some(builder.build())
            }
        };

        Self {
            inner,
            database_cache,
            table_cache,
            view_cache,
            loaded_table_cache,
        }
    }

    pub fn get_cache_bundle(&self) -> Arc<CatalogCacheBundle> {
        Arc::new(CatalogCacheBundle {
            database_cache: self.database_cache.clone(),
            table_cache: self.table_cache.clone(),
            view_cache: self.view_cache.clone(),
            loaded_table_cache: self.loaded_table_cache.clone(),
        })
    }
}

impl CatalogCacheBundle {
    pub fn new(config: &CatalogCacheConfig) -> Self {
        let database_cache = if matches!(
            config.database_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.database_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.database_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let table_cache = if matches!(
            config.table_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.table_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.table_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        let loaded_table_cache = if matches!(
            config.table_cache_type,
            sail_common::config::CacheType::Global
        ) {
            Some(LoadedTableCache::new(
                config.table_cache_size,
                config.table_cache_ttl_secs,
            ))
        } else {
            None
        };

        let view_cache = if matches!(
            config.view_cache_type,
            sail_common::config::CacheType::Global
        ) {
            let mut builder = Cache::builder().eviction_policy(EvictionPolicy::lru());
            if let Some(size) = config.view_cache_size.filter(|&s| s > 0) {
                builder = builder.max_capacity(size as u64);
            }
            if let Some(ttl) = config.view_cache_ttl_secs.filter(|&t| t > 0) {
                builder = builder.time_to_live(Duration::from_secs(ttl));
            }
            Some(builder.build())
        } else {
            None
        };

        Self {
            database_cache,
            table_cache,
            view_cache,
            loaded_table_cache,
        }
    }
}

#[async_trait::async_trait]
impl<P: CatalogProvider + ?Sized + 'static> CatalogProvider for CachingCatalogProvider<P> {
    fn get_name(&self) -> &str {
        self.inner.get_name()
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let status = self.inner.create_database(database, options).await?;
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            c.invalidate_all();
        }
        Ok(status)
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        self.inner.get_database(database).await
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            let key = prefix.cloned();
            log::debug!("CachingCatalogProvider::list_databases(prefix={:?})", key);
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_databases(prefix).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_databases(prefix).await
        }
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_database(database, options).await?;
        if let Some(c) = self.database_cache.as_ref() {
            let c: &Cache<Option<Namespace>, Vec<DatabaseStatus>> = c;
            c.invalidate_all();
        }
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        if let Some(c) = self.loaded_table_cache.as_ref() {
            c.invalidate_database(database);
        }
        Ok(())
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        if let Some(c) = self.loaded_table_cache.as_ref() {
            c.invalidate_table(database, table).await;
        }
        Ok(status)
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        self.inner.create_table_metadata_requirement(options)
    }

    fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
        self.inner.lakehouse_capabilities()
    }

    async fn resolve_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: ResolveLakehouseTableRequest,
    ) -> CatalogResult<LakehouseResolvedTable> {
        let Some(c) = self.loaded_table_cache.as_ref() else {
            return self
                .inner
                .resolve_lakehouse_table(database, table, request)
                .await;
        };
        if request.operation != LakehouseOperation::Read {
            // Anything but a read plans against the current table metadata.
            c.invalidate_table(database, table).await;
            return self
                .inner
                .resolve_lakehouse_table(database, table, request)
                .await;
        }
        let key = (LoadedTableCache::key(database, table), request.clone());
        if let Some(v) = c.resolved.get(&key).await {
            return Ok(v);
        }
        let generation = c.generation();
        let v = self
            .inner
            .resolve_lakehouse_table(database, table, request)
            .await?;
        c.insert_loaded(&c.resolved, key, v.clone(), generation)
            .await;
        Ok(v)
    }

    async fn plan_lakehouse_create(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCreateRequest,
    ) -> CatalogResult<LakehouseCreatePlan> {
        self.inner
            .plan_lakehouse_create(database, table, request)
            .await
    }

    async fn begin_table_access(
        &self,
        database: &Namespace,
        table: &str,
        request: BeginTableAccessRequest,
    ) -> CatalogResult<TableAccessSession> {
        let c = match self.loaded_table_cache.as_ref() {
            Some(c)
                if matches!(
                    request.purpose,
                    TableAccessPurpose::DataRead | TableAccessPurpose::ScanPlanning
                ) =>
            {
                c
            }
            _ => {
                return self
                    .inner
                    .begin_table_access(database, table, request)
                    .await;
            }
        };
        let key = (LoadedTableCache::key(database, table), request.clone());
        if let Some(v) = c.access.get(&key).await {
            return Ok(v);
        }
        let generation = c.generation();
        let v = self
            .inner
            .begin_table_access(database, table, request)
            .await?;
        // A session whose credentials are about to expire is not worth caching.
        if v.expires_at_ms
            .is_none_or(|ms| !table_access_remaining(ms).is_zero())
        {
            c.insert_loaded(&c.access, key, v.clone(), generation).await;
        }
        Ok(v)
    }

    async fn plan_lakehouse_scan(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseScanPlanningRequest,
    ) -> CatalogResult<LakehouseScanPlanningResponse> {
        self.inner
            .plan_lakehouse_scan(database, table, request)
            .await
    }

    async fn commit_lakehouse_table(
        &self,
        database: &Namespace,
        table: &str,
        request: LakehouseCommitRequest,
    ) -> CatalogResult<LakehouseCommitOutcome> {
        let outcome = self
            .inner
            .commit_lakehouse_table(database, table, request)
            .await;
        // Invalidate even when the commit fails: a conflict means the cached metadata is stale.
        if let Some(c) = self.loaded_table_cache.as_ref() {
            c.invalidate_table(database, table).await;
        }
        let outcome = outcome?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(outcome)
    }

    async fn get_delta_ratified_commits(
        &self,
        database: &Namespace,
        table: &str,
        request: DeltaRatifiedCommitRequest,
    ) -> CatalogResult<DeltaRatifiedCommitResponse> {
        self.inner
            .get_delta_ratified_commits(database, table, request)
            .await
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        if let Some(c) = self.loaded_table_cache.as_ref() {
            let key = LoadedTableCache::key(database, table);
            if let Some(v) = c.status.get(&key).await {
                return Ok(v);
            }
            let generation = c.generation();
            let v = self.inner.get_table(database, table).await?;
            c.insert_loaded(&c.status, key, v.clone(), generation).await;
            Ok(v)
        } else {
            self.inner.get_table(database, table).await
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            let key = database.clone();
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_tables(database).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_tables(database).await
        }
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_table(database, table, options).await?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        if let Some(c) = self.loaded_table_cache.as_ref() {
            c.invalidate_table(database, table).await;
        }
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        let result = self.inner.alter_table(database, table, options).await;
        // Invalidate even when the change fails: a rejected metadata location swap means
        // the cached metadata is stale.
        if let Some(c) = self.loaded_table_cache.as_ref() {
            c.invalidate_table(database, table).await;
        }
        result?;
        if let Some(c) = self.table_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let status = self.inner.create_view(database, view, options).await?;
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(status)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        self.inner.get_view(database, view).await
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            let key = database.clone();
            if let Some(v) = c.get(&key).await {
                return Ok(v);
            }
            let v = self.inner.list_views(database).await?;
            c.insert(key, v.clone()).await;
            Ok(v)
        } else {
            self.inner.list_views(database).await
        }
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        self.inner.drop_view(database, view, options).await?;
        if let Some(c) = self.view_cache.as_ref() {
            let c: &Cache<Namespace, Vec<TableStatus>> = c;
            c.invalidate(database).await;
        }
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sail_common_datafusion::catalog::{
        CatalogProviderId, CatalogTableIdentity, CommitAuthority, DatabaseStatus,
        LakehouseAuthority, LakehouseExecutionContext, LakehouseFormat, LakehouseOperation,
        MetadataPointerAuthority, ScanAuthority, TableKind, TableLifecycle, TableStatus,
    };

    use super::*;
    use crate::lakehouse::TableAccessPurpose;
    use crate::provider::{CreateDatabaseOptions, CreateTableOptions, Namespace};

    struct MockProvider {
        db_calls: AtomicUsize,
        table_calls: AtomicUsize,
        view_calls: AtomicUsize,
        access_calls: AtomicUsize,
        scan_calls: AtomicUsize,
        commit_calls: AtomicUsize,
        delta_commit_calls: AtomicUsize,
        get_table_calls: AtomicUsize,
        last_commit_format: Mutex<Option<String>>,
        access_expires_at_ms: Option<i64>,
    }

    impl MockProvider {
        fn new() -> Self {
            Self::with_access_expiry(Some(123))
        }

        fn with_access_expiry(access_expires_at_ms: Option<i64>) -> Self {
            Self {
                db_calls: AtomicUsize::new(0),
                table_calls: AtomicUsize::new(0),
                view_calls: AtomicUsize::new(0),
                access_calls: AtomicUsize::new(0),
                scan_calls: AtomicUsize::new(0),
                commit_calls: AtomicUsize::new(0),
                delta_commit_calls: AtomicUsize::new(0),
                get_table_calls: AtomicUsize::new(0),
                last_commit_format: Mutex::new(None),
                access_expires_at_ms,
            }
        }
    }

    #[async_trait::async_trait]
    impl CatalogProvider for MockProvider {
        fn get_name(&self) -> &str {
            "mock"
        }

        fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
            vec![
                LakehouseCapability::TableAccessSessions,
                LakehouseCapability::CatalogCommit,
                LakehouseCapability::DeltaRatifiedCommits,
            ]
        }

        async fn create_database(
            &self,
            database: &Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "cat".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "cat".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            _prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            self.db_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![DatabaseStatus {
                catalog: "cat".to_string(),
                database: vec!["db1".to_string()],
                comment: None,
                location: None,
                properties: vec![],
            }])
        }

        async fn drop_database(
            &self,
            _database: &Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn create_table(
            &self,
            database: &Namespace,
            table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: table.to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            })
        }

        async fn begin_table_access(
            &self,
            _database: &Namespace,
            _table: &str,
            request: BeginTableAccessRequest,
        ) -> CatalogResult<TableAccessSession> {
            self.access_calls.fetch_add(1, Ordering::SeqCst);
            Ok(TableAccessSession {
                reference: sail_common_datafusion::catalog::TableAccessSessionRef {
                    fingerprint: format!("{:?}", request.purpose),
                },
                capability_fingerprint: request.context.capability_fingerprint.clone(),
                context: request.context,
                expires_at_ms: self.access_expires_at_ms,
                credential_scope: Some("test-scope".to_string()),
            })
        }

        async fn plan_lakehouse_scan(
            &self,
            _database: &Namespace,
            _table: &str,
            request: LakehouseScanPlanningRequest,
        ) -> CatalogResult<LakehouseScanPlanningResponse> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(LakehouseScanPlanningResponse {
                authority: request.context.scan,
                files: Some(request.filters),
                residual_filter: None,
                payload: Some(serde_json::json!({
                    "projection": request.projection,
                    "limit": request.limit,
                })),
            })
        }

        async fn commit_lakehouse_table(
            &self,
            _database: &Namespace,
            _table: &str,
            request: LakehouseCommitRequest,
        ) -> CatalogResult<LakehouseCommitOutcome> {
            self.commit_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_commit_format.lock().unwrap() = Some(request.format.clone());
            Ok(LakehouseCommitOutcome::Committed {
                context: request.context,
                payload: request.payload,
            })
        }

        async fn get_delta_ratified_commits(
            &self,
            _database: &Namespace,
            _table: &str,
            request: DeltaRatifiedCommitRequest,
        ) -> CatalogResult<DeltaRatifiedCommitResponse> {
            self.delta_commit_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DeltaRatifiedCommitResponse {
                latest_table_version: request.start_version,
                commits: vec![],
            })
        }

        async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
            self.get_table_calls.fetch_add(1, Ordering::SeqCst);
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: table.to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            })
        }

        async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            self.table_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: "t1".to_string(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: None,
                    format: "parquet".to_string(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: vec![],
                    is_external: false,
                },
            }])
        }

        async fn drop_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn alter_table(
            &self,
            _database: &Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }

        async fn create_view(
            &self,
            database: &Namespace,
            view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: view.to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }

        async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: view.to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            })
        }

        async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            self.view_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TableStatus {
                catalog: Some("cat".to_string()),
                database: database.clone().into(),
                name: "v1".to_string(),
                kind: TableKind::View {
                    definition: "SELECT 1".to_string(),
                    columns: vec![],
                    comment: None,
                    properties: vec![],
                },
            }])
        }

        async fn drop_view(
            &self,
            _database: &Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            Ok(())
        }
    }

    fn mock_provider() -> Arc<MockProvider> {
        Arc::new(MockProvider::new())
    }

    fn test_lakehouse_context() -> LakehouseExecutionContext {
        LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("mock".to_string()),
            vec!["cat".to_string(), "db1".to_string(), "t1".to_string()],
            CatalogTableIdentity {
                table_id: Some("table-id".to_string()),
                table_uri: Some("file:///tmp/table".to_string()),
            },
            LakehouseOperation::Write,
            LakehouseFormat::Delta,
            LakehouseAuthority::CatalogAuthoritative {
                lifecycle: TableLifecycle::Managed,
                pointer: MetadataPointerAuthority::DeltaRatifiedCommits,
                commit: CommitAuthority::DeltaRatifiedCommit,
            },
            ScanAuthority::ClientLakeSource,
        )
    }

    #[tokio::test]
    async fn test_caching_behavior() {
        let mock = mock_provider();
        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            database_cache_size: Some(10),
            database_cache_ttl_secs: Some(60),
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(10),
            table_cache_ttl_secs: Some(60),
            view_cache_type: sail_common::config::CacheType::Session,
            view_cache_size: Some(10),
            view_cache_ttl_secs: Some(60),
        };
        let provider = CachingCatalogProvider::new(mock.clone(), config, None);

        // First call - should hit mock
        let dbs = provider.list_databases(None).await.unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // Second call - should hit cache
        let dbs = provider.list_databases(None).await.unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // First table call - should hit mock
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 1);

        // Second table call - should hit cache
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 1);

        // First view call - should hit mock
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 1);

        // Second view call - should hit cache
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 1);

        // Invalidate cache via create_table
        let options = CreateTableOptions {
            columns: vec![],
            comment: None,
            constraints: vec![],
            location: None,
            format: "parquet".to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode: crate::provider::CreateTableMode::Create,
            properties: vec![],
            is_external: false,
            is_write_precondition: false,
        };
        provider.create_table(&ns, "t2", options).await.unwrap();

        // Third table call - should hit mock again
        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 2);

        // Invalidate view cache via create_view
        let view_options = CreateViewOptions {
            columns: vec![],
            definition: "SELECT 1".to_string(),
            if_not_exists: false,
            replace: false,
            comment: None,
            properties: vec![],
        };
        provider.create_view(&ns, "v2", view_options).await.unwrap();

        // Third view call - should hit mock again
        let views = provider.list_views(&ns).await.unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 2);

        // Test get methods (direct pass-through)
        provider.get_database(&ns).await.unwrap();
        // Call get_table and get_view as well to exercise the wrapper pass-through paths.
        let _ = provider.get_table(&ns, "t1").await;
        let _ = provider.get_view(&ns, "v1").await;

        // Test alter table invalidation
        provider
            .alter_table(
                &ns,
                "t1",
                AlterTableOptions::SetTableProperties { properties: vec![] },
            )
            .await
            .unwrap();
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 3);

        // Test drop table invalidation
        provider
            .drop_table(
                &ns,
                "t1",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await
            .unwrap();
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 4);

        // Test drop view invalidation
        provider
            .drop_view(&ns, "v1", DropViewOptions { if_exists: false })
            .await
            .unwrap();
        let _ = provider.list_views(&ns).await.unwrap();
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 3);

        // Test drop database invalidation (invalidates all)
        provider
            .drop_database(
                &ns,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: false,
                },
            )
            .await
            .unwrap();
        let _ = provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 2);
        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 5);
        let _ = provider.list_views(&ns).await.unwrap();
        assert_eq!(mock.view_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_catalog_cache_manager() {
        let manager = CatalogCacheManager::new();
        let name = "test_catalog".to_string();

        // Initially no cache
        assert!(manager.get_cache(&name).unwrap().is_none());

        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            table_cache_type: sail_common::config::CacheType::Session,
            ..Default::default()
        };
        // Use a dummy provider to get a bundle
        let mock = mock_provider();
        let provider = CachingCatalogProvider::new(mock, config, None);
        let bundle = provider.get_cache_bundle();

        manager.set_cache(name.clone(), bundle).unwrap();

        // Now cache should exist
        let retrieved = manager.get_cache(&name).unwrap();
        assert!(retrieved.is_some());
        let bundle = retrieved.unwrap();
        assert!(bundle.database_cache.is_some());
        assert!(bundle.table_cache.is_some());
    }
    #[tokio::test]
    async fn test_cache_config_normalization() {
        let mock = mock_provider();
        // Config with 0 values - should be treated as unbounded
        let config = CatalogCacheConfig {
            database_cache_type: sail_common::config::CacheType::Session,
            database_cache_size: Some(0),
            database_cache_ttl_secs: Some(0),
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(0),
            table_cache_ttl_secs: Some(0),
            ..Default::default()
        };
        let provider = CachingCatalogProvider::new(mock.clone(), config, None);

        // Verification: if it was treated as max_capacity(0), it wouldn't cache anything.
        // We can't easily inspect the internal Moka cache settings, but we can verify behavior.

        // First call
        provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);

        // Second call - should hit cache (meaning max_capacity(0) was NOT applied)
        provider.list_databases(None).await.unwrap();
        assert_eq!(mock.db_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lakehouse_methods_forward_and_commit_invalidates_table_cache() {
        let mock = mock_provider();
        let config = CatalogCacheConfig {
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(10),
            table_cache_ttl_secs: Some(60),
            ..Default::default()
        };
        let provider = CachingCatalogProvider::new(mock.clone(), config, None);
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        let context = test_lakehouse_context();

        let tables = provider.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 1);

        let access = provider
            .begin_table_access(
                &ns,
                "t1",
                BeginTableAccessRequest {
                    context: context.clone(),
                    purpose: TableAccessPurpose::DataRead,
                },
            )
            .await
            .unwrap();
        assert_eq!(access.context, context);
        assert_eq!(access.reference.fingerprint, "DataRead");
        assert_eq!(mock.access_calls.load(Ordering::SeqCst), 1);

        let scan = provider
            .plan_lakehouse_scan(
                &ns,
                "t1",
                LakehouseScanPlanningRequest {
                    context: context.clone(),
                    filters: vec![serde_json::json!({"op": "always_true"})],
                    projection: Some(vec!["id".to_string()]),
                    limit: Some(5),
                },
            )
            .await
            .unwrap();
        assert_eq!(scan.authority, ScanAuthority::ClientLakeSource);
        assert_eq!(mock.scan_calls.load(Ordering::SeqCst), 1);

        let delta_commits = provider
            .get_delta_ratified_commits(
                &ns,
                "t1",
                DeltaRatifiedCommitRequest {
                    context: context.clone(),
                    table_uri: "file:///tmp/table".to_string(),
                    start_version: 7,
                    end_version: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(delta_commits.latest_table_version, 7);
        assert_eq!(mock.delta_commit_calls.load(Ordering::SeqCst), 1);

        let outcome = provider
            .commit_lakehouse_table(
                &ns,
                "t1",
                LakehouseCommitRequest {
                    context: context.clone(),
                    format: "delta".to_string(),
                    requirements: vec![],
                    updates: vec![],
                    payload: Some(serde_json::json!({"ok": true})),
                },
            )
            .await
            .unwrap();
        assert!(matches!(outcome, LakehouseCommitOutcome::Committed { .. }));
        assert_eq!(mock.commit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mock.last_commit_format.lock().unwrap().as_deref(),
            Some("delta")
        );

        let _ = provider.list_tables(&ns).await.unwrap();
        assert_eq!(mock.table_calls.load(Ordering::SeqCst), 2);
    }

    fn loaded_table_cache_config() -> CatalogCacheConfig {
        CatalogCacheConfig {
            table_cache_type: sail_common::config::CacheType::Session,
            table_cache_size: Some(10),
            table_cache_ttl_secs: Some(60),
            ..Default::default()
        }
    }

    fn resolve_request(table: &str, operation: LakehouseOperation) -> ResolveLakehouseTableRequest {
        ResolveLakehouseTableRequest {
            catalog_table: vec!["cat".to_string(), "db1".to_string(), table.to_string()],
            operation,
            requested_format: None,
            options: vec![],
        }
    }

    /// Loads a table the way the planner does for a read: get the table, resolve it,
    /// and begin a table access session for it.
    async fn read_table<P: CatalogProvider + ?Sized + 'static>(
        provider: &CachingCatalogProvider<P>,
        ns: &Namespace,
        table: &str,
    ) {
        provider.get_table(ns, table).await.unwrap();
        let resolved = provider
            .resolve_lakehouse_table(ns, table, resolve_request(table, LakehouseOperation::Read))
            .await
            .unwrap();
        provider
            .begin_table_access(
                ns,
                table,
                BeginTableAccessRequest {
                    context: resolved.execution,
                    purpose: TableAccessPurpose::DataRead,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_loaded_table_cache() {
        let mock = Arc::new(MockProvider::with_access_expiry(None));
        let provider = CachingCatalogProvider::new(mock.clone(), loaded_table_cache_config(), None);
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        let get_table_calls = || mock.get_table_calls.load(Ordering::SeqCst);
        let access_calls = || mock.access_calls.load(Ordering::SeqCst);

        // The first read loads the table twice (`get_table`, then the inner provider's
        // `resolve_lakehouse_table`) and begins one table access session.
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 2);
        assert_eq!(access_calls(), 1);

        // Later reads are served from the cache.
        read_table(&provider, &ns, "t1").await;
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 2);
        assert_eq!(access_calls(), 1);

        read_table(&provider, &ns, "t2").await;
        assert_eq!(get_table_calls(), 4);
        assert_eq!(access_calls(), 2);

        // Resolving a table for a write bypasses the cache and drops that table's entries.
        provider
            .resolve_lakehouse_table(&ns, "t1", resolve_request("t1", LakehouseOperation::Write))
            .await
            .unwrap();
        assert_eq!(get_table_calls(), 5);
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 7);
        assert_eq!(access_calls(), 3);

        // A table access session for a write is never cached.
        for _ in 0..2 {
            provider
                .begin_table_access(
                    &ns,
                    "t1",
                    BeginTableAccessRequest {
                        context: test_lakehouse_context(),
                        purpose: TableAccessPurpose::DataWrite,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(access_calls(), 5);

        // A commit drops the entries of the committed table only.
        provider
            .commit_lakehouse_table(
                &ns,
                "t1",
                LakehouseCommitRequest {
                    context: test_lakehouse_context(),
                    format: "iceberg".to_string(),
                    requirements: vec![],
                    updates: vec![],
                    payload: None,
                },
            )
            .await
            .unwrap();
        read_table(&provider, &ns, "t2").await;
        assert_eq!(get_table_calls(), 7);
        assert_eq!(access_calls(), 5);
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 9);
        assert_eq!(access_calls(), 6);

        provider
            .alter_table(
                &ns,
                "t1",
                AlterTableOptions::SetTableProperties { properties: vec![] },
            )
            .await
            .unwrap();
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 11);
        assert_eq!(access_calls(), 7);

        provider
            .drop_table(
                &ns,
                "t1",
                DropTableOptions {
                    if_exists: false,
                    purge: false,
                },
            )
            .await
            .unwrap();
        read_table(&provider, &ns, "t1").await;
        assert_eq!(get_table_calls(), 13);
        assert_eq!(access_calls(), 8);

        // Dropping the database drops the entries of every table in it.
        provider
            .drop_database(
                &ns,
                DropDatabaseOptions {
                    if_exists: false,
                    cascade: true,
                },
            )
            .await
            .unwrap();
        read_table(&provider, &ns, "t1").await;
        read_table(&provider, &ns, "t2").await;
        assert_eq!(get_table_calls(), 17);
        assert_eq!(access_calls(), 10);
    }

    #[tokio::test]
    async fn test_loaded_table_cache_respects_access_expiry() {
        let ns = Namespace::try_from(vec!["db1"]).unwrap();

        // Credentials that expired long ago are never cached.
        let mock = Arc::new(MockProvider::with_access_expiry(Some(123)));
        let provider = CachingCatalogProvider::new(mock.clone(), loaded_table_cache_config(), None);
        read_table(&provider, &ns, "t1").await;
        read_table(&provider, &ns, "t1").await;
        assert_eq!(mock.get_table_calls.load(Ordering::SeqCst), 2);
        assert_eq!(mock.access_calls.load(Ordering::SeqCst), 2);

        // Credentials valid for an hour are cached.
        let mock = Arc::new(MockProvider::with_access_expiry(Some(
            now_ms() + 60 * 60 * 1000,
        )));
        let provider = CachingCatalogProvider::new(mock.clone(), loaded_table_cache_config(), None);
        read_table(&provider, &ns, "t1").await;
        read_table(&provider, &ns, "t1").await;
        assert_eq!(mock.access_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_loaded_table_cache_drops_loads_that_race_an_invalidation() {
        let cache = LoadedTableCache::new(None, Some(60));
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        let key = LoadedTableCache::key(&ns, "t1");
        let status = MockProvider::new().get_table(&ns, "t1").await.unwrap();

        // A load that started before an invalidation of any table is not cached.
        let generation = cache.generation();
        cache.invalidate_table(&ns, "t1").await;
        cache
            .insert_loaded(&cache.status, key.clone(), status.clone(), generation)
            .await;
        assert!(cache.status.get(&key).await.is_none());

        let generation = cache.generation();
        cache.invalidate_database(&ns);
        cache
            .insert_loaded(&cache.status, key.clone(), status.clone(), generation)
            .await;
        assert!(cache.status.get(&key).await.is_none());

        // A load with no invalidation in between is cached.
        let generation = cache.generation();
        cache
            .insert_loaded(&cache.status, key.clone(), status, generation)
            .await;
        assert!(cache.status.get(&key).await.is_some());
    }

    #[test]
    fn test_loaded_table_cache_always_expires() {
        let default_ttl = Some(Duration::from_secs(DEFAULT_LOADED_TABLE_TTL_SECS));
        for ttl_secs in [None, Some(0)] {
            let cache = LoadedTableCache::new(None, ttl_secs);
            assert_eq!(cache.status.policy().time_to_live(), default_ttl);
            assert_eq!(cache.resolved.policy().time_to_live(), default_ttl);
            assert_eq!(cache.access.policy().time_to_live(), default_ttl);
        }
        let cache = LoadedTableCache::new(None, Some(3600));
        assert_eq!(
            cache.status.policy().time_to_live(),
            Some(Duration::from_secs(3600))
        );
    }

    #[tokio::test]
    async fn test_loaded_table_cache_disabled() {
        let mock = Arc::new(MockProvider::with_access_expiry(None));
        let provider =
            CachingCatalogProvider::new(mock.clone(), CatalogCacheConfig::default(), None);
        let ns = Namespace::try_from(vec!["db1"]).unwrap();
        read_table(&provider, &ns, "t1").await;
        read_table(&provider, &ns, "t1").await;
        assert_eq!(mock.get_table_calls.load(Ordering::SeqCst), 4);
        assert_eq!(mock.access_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_loaded_table_cache_is_shared_through_global_bundle() {
        let config = CatalogCacheConfig {
            table_cache_type: sail_common::config::CacheType::Global,
            ..loaded_table_cache_config()
        };
        let bundle = Arc::new(CatalogCacheBundle::new(&config));
        assert!(bundle.loaded_table_cache.is_some());
        let mock = Arc::new(MockProvider::with_access_expiry(None));
        let ns = Namespace::try_from(vec!["db1"]).unwrap();

        let first = CachingCatalogProvider::new(mock.clone(), config.clone(), Some(bundle.clone()));
        read_table(&first, &ns, "t1").await;
        let second = CachingCatalogProvider::new(mock.clone(), config, Some(bundle));
        read_table(&second, &ns, "t1").await;
        assert_eq!(mock.get_table_calls.load(Ordering::SeqCst), 2);
        assert_eq!(mock.access_calls.load(Ordering::SeqCst), 1);
    }
}
