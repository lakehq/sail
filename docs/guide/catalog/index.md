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

The next few pages describe the different catalog providers supported by Sail and how to configure them.

## Common Options

All remote catalog providers (excluding the [Memory catalog](./memory)) support the following common options for caching database and table listings. This is particularly useful for reducing the number of requests to external services.

- `database_cache_type` (optional): The scope of the database listing cache. Valid values are `none`, `global`, which shares the cache across sessions, and `session`, which keeps the cache private to one session. The default is `none`.
- `database_cache_size` (optional): The maximum number of entries in the database listing cache. Set this option to `0` for an unbounded cache.
- `database_cache_ttl_secs` (optional): The time-to-live for cached database listings. Set this option in seconds. Set it to `0` to disable expiration.
- `table_cache_type` (optional): The scope of the table listing cache. Valid values are `none`, `global`, and `session`. The default is `none`.
- `table_cache_size` (optional): The maximum number of entries in the table listing cache. Set this option to `0` for an unbounded cache.
- `table_cache_ttl_secs` (optional): The time-to-live for cached table listings. Set this option in seconds. Set it to `0` to disable expiration.
- `view_cache_type` (optional): The scope of the view listing cache. Valid values are `none`, `global`, and `session`. The default is `none`.
- `view_cache_size` (optional): The maximum number of entries in the view listing cache. Set this option to `0` for an unbounded cache.
- `view_cache_ttl_secs` (optional): The time-to-live for cached view listings. Set this option in seconds. Set it to `0` to disable expiration.

The cache is automatically invalidated when a write operation (like `CREATE TABLE` or `DROP DATABASE`) is performed through Sail.

## Support Matrix

Here is a list of the supported (:white_check_mark:) catalog providers and the ones that are planned in our roadmap (:construction:).

| Catalog Provider               | Supported          |
| ------------------------------ | ------------------ |
| [Memory](./memory)             | :white_check_mark: |
| [Iceberg REST](./iceberg-rest) | :white_check_mark: |
| [Unity Catalog](./unity)       | :white_check_mark: |
| [AWS Glue](./glue)             | :white_check_mark: |
| [OneLake](./onelake)           | :white_check_mark: |
| [Hive Metastore](./hms)        | :white_check_mark: |
