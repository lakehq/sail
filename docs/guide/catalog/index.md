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

| Catalog Provider               | Supported          |
| ------------------------------ | ------------------ |
| [Memory](./memory)             | :white_check_mark: |
| [Iceberg REST](./iceberg-rest) | :white_check_mark: |
| [Unity Catalog](./unity)       | :white_check_mark: |
| [AWS Glue](./glue)             | :white_check_mark: |
| [OneLake](./onelake)           | :white_check_mark: |
| Hive Metastore                 | :construction:     |
