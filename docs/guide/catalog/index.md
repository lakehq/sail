---
title: Catalog
rank: 7
---

# Catalog

Sail supports various catalog providers to manage your datasets as external tables. Catalogs help organize and maintain metadata about your data, so that you can refer to them by table names in your SQL queries.

By default, Sail uses a memory catalog provider that stores table metadata in memory for the duration of your session.
You can configure remote catalog providers to persist your table metadata across sessions. This is done using the Sail configuration options.

For example, you can configure memory catalogs using the `catalog.list` option and set the default catalog using the `catalog.default_catalog` option. The configuration can be done via environment variables before starting the Sail server.

```bash
export SAIL_CATALOG__LIST='[{name="c1", type="memory", initial_database=["default"]}, {name="c2", type="iceberg-rest", uri="https://catalog.example.com"}]'
export SAIL_CATALOG__DEFAULT_CATALOG="c1"
```

Then you can interact with the catalogs using the Spark API.

<!--@include: ../_common/spark-session.md-->

```python
spark.catalog.listCatalogs()
spark.catalog.currentCatalog()
spark.catalog.listTables()
spark.catalog.setCurrentCatalog("c2")
```

You can also interact with catalogs using SQL statements.

```sql
-- show the current catalog
SELECT current_catalog()
-- show tables in the current catalog
SHOW TABLES
```

In the next few pages, we will explore the different catalog providers supported by Sail and how to configure them.

## Support Matrix

Here is a list of the supported (:white_check_mark:) catalog providers and the ones that are planned in our roadmap (:construction:).

| Catalog Provider               | Supported          | Caching            |
| ------------------------------ | ------------------ | ------------------ |
| [Memory](./memory)             | :white_check_mark: | N/A                |
| [Iceberg REST](./iceberg-rest) | :white_check_mark: | :white_check_mark: |
| [Unity Catalog](./unity)       | :white_check_mark: | :white_check_mark: |
| [AWS Glue](./glue)             | :white_check_mark: | :white_check_mark: |
| [OneLake](./onelake)           | :white_check_mark: | :white_check_mark: |
| [Hive Metastore](./hms)        | :white_check_mark: | :white_check_mark: |

## Caching

All remote catalog providers share a common caching layer that accelerates `list_databases`, `list_tables`, and `list_views` operations. Caching is opt-in and configured per catalog instance.

### Configuration

The following cache options can be set alongside any catalog provider options:

| Option                      | Type    | Default | Description                                                               |
| --------------------------- | ------- | ------- | ------------------------------------------------------------------------- |
| `database_cache_enabled`    | boolean | `false` | Whether to enable caching for database listings.                          |
| `database_cache_size`       | number  | -       | Maximum number of entries in the database list cache. `0` = unbounded.    |
| `database_cache_ttl_secs`   | number  | -       | Time-to-live in seconds for cached database listings. `0` = no expiry.    |
| `table_cache_enabled`       | boolean | `false` | Whether to enable caching for table and view listings.                    |
| `table_cache_size`          | number  | -       | Maximum number of entries in the table/view list cache. `0` = unbounded.  |
| `table_cache_ttl_secs`      | number  | -       | Time-to-live in seconds for cached table/view listings. `0` = no expiry.  |

Example:

```bash
export SAIL_CATALOG__LIST='[{type="glue", name="sail", region="us-east-1", database_cache_enabled=true, table_cache_enabled=true, database_cache_ttl_secs=3600, table_cache_ttl_secs=1800}]'
```

### Cache Invalidation

When a mutation is performed through Sail, the relevant cache entries are automatically invalidated:

| Mutation             | Invalidated caches                        |
| -------------------- | ----------------------------------------- |
| `create_database`    | database list cache                       |
| `drop_database`      | database list cache + table + view caches |
| `create_table`       | table list cache for the database         |
| `drop_table`         | table list cache for the database         |
| `alter_table`        | table list cache for the database         |
| `create_view`        | view list cache for the database          |
| `drop_view`          | view list cache for the database          |

Read-only operations (`get_database`, `get_table`, `get_view`) are **not** cached and always hit the remote catalog directly. This is a deliberate design choice because individual lookups are fast and caching them would complicate invalidation logic.

## Developer Guide: Adding Caching to a New Provider

Caching is implemented as a **decorator pattern** and lives entirely outside of individual catalog providers. When adding a new provider, you do not write any caching logic inside the provider itself. Instead, you wire the existing generic caching wrapper around it.

### Architecture Overview

```
User request
    |
    v
CachedCatalogProvider          <-- generic cache wrapper (sail-catalog/src/provider/cached.rs)
    |
    v
RuntimeAwareCatalogProvider    <-- optional: bridges sync/async runtimes
    |
    v
YourCatalogProvider            <-- your provider, implements CatalogProvider trait
    |
    v
Remote service (Glue, HMS, REST, ...)
```

The key design principle is **separation of concerns**: your provider handles only the remote service interaction, while `CachedCatalogProvider` handles all caching transparently.

### Step-by-Step Checklist

Here is a step-by-step guide to adding caching support for a new catalog provider, using the AWS Glue implementation as a reference.

#### Step 1: Implement the `CatalogProvider` trait

Your provider must implement `CatalogProvider` from `sail-catalog/src/provider/mod.rs`. It should have **zero** caching logic internally. All methods interact directly with the remote service.

```rust
// crates/sail-catalog-your-service/src/provider.rs

#[async_trait]
impl CatalogProvider for YourCatalogProvider {
    fn get_name(&self) -> &str { &self.name }

    async fn create_database(&self, database: &Namespace, options: CreateDatabaseOptions)
        -> CatalogResult<DatabaseStatus> {
        // Direct call to your remote service SDK
    }

    async fn list_databases(&self, prefix: Option<&Namespace>)
        -> CatalogResult<Vec<DatabaseStatus>> {
        // Direct call to your remote service SDK
    }

    // ... implement all 14 trait methods
}
```

See `crates/sail-catalog-glue/src/provider.rs` for a complete reference implementation.

#### Step 2: Add the `cache` field to `CatalogType`

In `crates/sail-common/src/config/application.rs`, add a variant to the `CatalogType` enum for your provider. Include the `cache` field using `#[serde(flatten)]`:

```rust
// In CatalogType enum
#[serde(alias = "your-service")]
YourService {
    name: String,
    // Your provider-specific fields
    uri: String,
    #[serde(flatten)]
    cache: CatalogCacheConfig,
},
```

The `#[serde(flatten)]` attribute inlines all `CatalogCacheConfig` fields (`database_cache_enabled`, `table_cache_enabled`, etc.) directly into the catalog configuration, so users do not need a nested `cache` object.

#### Step 3: Wire the provider in `sail-session`

In `crates/sail-session/src/catalog.rs`, add a match arm for your provider in `create_catalog_providers`. Use the existing `wrap_catalog` function to apply caching:

```rust
// In create_catalog_providers match block
CatalogType::YourService { name, uri, cache } => {
    let runtime_aware = RuntimeAwareCatalogProvider::try_new(
        || Ok(YourCatalogProvider::new(name.to_string(), uri.to_string())),
        runtime.io().clone(),
    )?;
    (name.to_string(), wrap_catalog(runtime_aware, cache))
}
```

The `wrap_catalog` function handles the conditional wrapping:

```rust
fn wrap_catalog<P: CatalogProvider + 'static>(
    provider: P,
    cache_config: &CatalogCacheConfig,
) -> Arc<dyn CatalogProvider> {
    let provider: Arc<dyn CatalogProvider> = Arc::new(provider);
    if cache_config.database_cache_enabled || cache_config.table_cache_enabled {
        Arc::new(CachedCatalogProvider::new(provider, cache_config))
    } else {
        provider
    }
}
```

If neither cache is enabled, no wrapper is added and there is zero overhead.

#### Step 4: Add documentation

Add a page for your provider under `docs/guide/catalog/` following the same structure as `glue.md`. Include the cache configuration options in your documentation.

### Key Rules

1. **Never add `moka` or any cache crate as a dependency of your provider crate.** Caching is handled by `sail-catalog` only.
2. **Never store caching state in your provider struct.** Your provider should be stateless with respect to caching.
3. **Always use `wrap_catalog` in the session wiring.** This ensures consistent behavior across all providers.
4. **Use `RuntimeAwareCatalogProvider` for async providers.** If your provider uses an async SDK (like AWS SDK, HTTP clients, etc.), wrap it with `RuntimeAwareCatalogProvider` before passing to `wrap_catalog`.

### Reference Files

| File | Description |
| ---- | ----------- |
| `crates/sail-catalog/src/provider/mod.rs` | `CatalogProvider` trait definition |
| `crates/sail-catalog/src/provider/cached.rs` | `CachedCatalogProvider` decorator implementation |
| `crates/sail-common/src/config/application.rs` | `CatalogCacheConfig` and `CatalogType` definitions |
| `crates/sail-session/src/catalog.rs` | Provider wiring with `wrap_catalog` |
| `crates/sail-catalog-glue/src/provider.rs` | Reference implementation (AWS Glue) |

