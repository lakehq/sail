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

The following catalog providers are available in Sail. Table DDL capabilities are listed below.

| Catalog Provider               |     Supported      |
| ------------------------------ | :----------------: |
| [Memory](./memory)             | :white_check_mark: |
| [Iceberg REST](./iceberg-rest) | :white_check_mark: |
| [Unity Catalog](./unity)       | :white_check_mark: |
| [AWS Glue](./glue)             | :white_check_mark: |
| [OneLake](./onelake)           | :white_check_mark: |
| [Hive Metastore](./hms)        | :white_check_mark: |

## Lakehouse DDL

These matrices cover table DDL, including operations that Sail does not yet implement.
The operation inventory follows [Delta Lake table DDL](https://docs.delta.io/delta-batch/)
and [Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/).

:white_check_mark: Supported · :warning: Supported with limitations · :construction: Not supported

`ALTER COLUMN` rows refer to top-level columns unless stated otherwise.
HMS means Hive Metastore. Iceberg REST includes servers such as Nessie and Lakekeeper.

### Delta Lake

| DDL operation                                                           |       Memory       |        HMS         |      AWS Glue      |  Iceberg REST  |  Unity (managed)   |  Unity (external)  |    OneLake     |
| ----------------------------------------------------------------------- | :----------------: | :----------------: | :----------------: | :------------: | :----------------: | :----------------: | :------------: |
| `CREATE TABLE [IF NOT EXISTS]`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE ... AS SELECT` (CTAS)                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE ... LOCATION` (register existing table)                   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: |   :construction:   | :white_check_mark: | :construction: |
| `CREATE TABLE ... PARTITIONED BY`                                       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE` with column defaults                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE` with generated columns                                   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE` with identity columns                                    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE` with `NOT NULL` columns                                  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `CREATE TABLE` with table/column comments                               | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `REPLACE TABLE` / `CREATE OR REPLACE TABLE`                             | :white_check_mark: |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `REPLACE TABLE ... AS SELECT` / `CREATE OR REPLACE TABLE ... AS SELECT` | :white_check_mark: |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `CREATE TABLE ... LIKE`                                                 |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `DROP TABLE [IF EXISTS]`                                                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: | :white_check_mark: | :construction: |
| `DROP TABLE ... PURGE` (delete data)                                    |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `TRUNCATE TABLE`                                                        |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... RENAME TO`                                             |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... ADD COLUMNS`                                           |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... DROP COLUMNS`                                          |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... RENAME COLUMN`                                         |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... REPLACE COLUMNS`                                       |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER/CHANGE COLUMN ... TYPE`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER/CHANGE COLUMN ... TYPE` (nested field)                           | :white_check_mark: |   :construction:   |   :construction:   | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER COLUMN ... SET DEFAULT`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER COLUMN ... DROP DEFAULT`                                         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER/CHANGE COLUMN ... COMMENT`                                       |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER/CHANGE COLUMN ... FIRST/AFTER`                                   |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER COLUMN ... SET/DROP NOT NULL`                                    |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER COLUMN ... SYNC IDENTITY`                                        |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... ADD CONSTRAINT ... CHECK`                              | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: |   :construction:   |     :warning:      | :construction: |
| `ALTER TABLE ... DROP CONSTRAINT`                                       |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... SET TBLPROPERTIES`                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER TABLE ... UNSET TBLPROPERTIES [IF EXISTS]`                       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :white_check_mark: |     :warning:      | :construction: |
| `ALTER TABLE ... SET LOCATION`                                          |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `COMMENT ON TABLE/COLUMN`                                               |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `CREATE/ALTER TABLE ... CLUSTER BY`                                     |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `ALTER TABLE ... DROP FEATURE`                                          |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `CREATE [OR REPLACE] TABLE ... SHALLOW CLONE`                           |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |
| `CONVERT TO DELTA`                                                      |   :construction:   |   :construction:   |   :construction:   | :construction: |   :construction:   |   :construction:   | :construction: |

Delta type widening requires `delta.enableTypeWidening=true` and a supported type
promotion. Nested type changes are available through Memory and Unity; HMS and Glue
support top-level type changes. Column defaults target top-level columns. Identity
columns must be non-partition `BIGINT` columns with a nonzero step.

Unity external Delta ALTER updates the Delta log. Its table API has no endpoint for
updating the external registration's columns or properties, so catalog-only
`DESCRIBE` and `SHOW TBLPROPERTIES` can retain their registered values. Managed Delta
schema and property changes are published through Unity's commit protocol;
`ADD CONSTRAINT` is not yet supported for managed tables.

### Iceberg

| DDL operation                                                           |       Memory       |        HMS         |      AWS Glue      |    Iceberg REST    |     Unity      |    OneLake     |
| ----------------------------------------------------------------------- | :----------------: | :----------------: | :----------------: | :----------------: | :------------: | :------------: |
| `CREATE TABLE [IF NOT EXISTS]`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `CREATE TABLE ... AS SELECT` (CTAS)                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `CREATE TABLE ... LOCATION` (register existing table)                   | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `CREATE TABLE ... PARTITIONED BY` (including transforms)                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `CREATE TABLE` with column defaults                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `CREATE TABLE` with `NOT NULL` columns                                  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `CREATE TABLE` with table/column comments                               | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `REPLACE TABLE` / `CREATE OR REPLACE TABLE`                             | :white_check_mark: |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `REPLACE TABLE ... AS SELECT` / `CREATE OR REPLACE TABLE ... AS SELECT` | :white_check_mark: |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `CREATE TABLE ... LIKE`                                                 |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `DROP TABLE [IF EXISTS]`                                                | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `DROP TABLE ... PURGE` (delete data)                                    |   :construction:   |   :construction:   |   :construction:   |     :warning:      | :construction: | :construction: |
| `TRUNCATE TABLE`                                                        |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... RENAME TO`                                             |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... ADD COLUMN(S)`                                         |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... DROP COLUMN(S)`                                        |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... RENAME COLUMN`                                         |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... REPLACE COLUMNS`                                       |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER/CHANGE COLUMN ... TYPE`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `ALTER/CHANGE COLUMN ... TYPE` (nested field)                           |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER COLUMN ... SET DEFAULT`                                          | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `ALTER COLUMN ... DROP DEFAULT`                                         | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `ALTER COLUMN ... COMMENT`                                              |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER COLUMN ... FIRST/AFTER`                                          |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER COLUMN ... DROP NOT NULL`                                        |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... SET TBLPROPERTIES`                                     | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :construction: | :construction: |
| `ALTER TABLE ... UNSET TBLPROPERTIES [IF EXISTS]`                       | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `ALTER TABLE ... SET TBLPROPERTIES ('format-version' = ...)` (upgrade)  | :white_check_mark: | :white_check_mark: | :white_check_mark: |     :warning:      | :construction: | :construction: |
| `ALTER TABLE ... SET LOCATION`                                          |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `COMMENT ON TABLE/COLUMN`                                               |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... ADD PARTITION FIELD`                                   |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... DROP PARTITION FIELD`                                  |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... REPLACE PARTITION FIELD`                               |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... WRITE ORDERED BY`                                      |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... WRITE LOCALLY ORDERED BY`                              |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... WRITE UNORDERED`                                       |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... WRITE DISTRIBUTED BY PARTITION`                        |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... SET/DROP IDENTIFIER FIELDS`                            |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... CREATE/REPLACE/DROP BRANCH`                            |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |
| `ALTER TABLE ... CREATE/REPLACE/DROP TAG`                               |   :construction:   |   :construction:   |   :construction:   |   :construction:   | :construction: | :construction: |

Iceberg type promotions permit `INT` to `BIGINT`, `FLOAT` to `DOUBLE`, and decimal
precision increases at the same scale. Type and default changes target top-level
columns. Column defaults require format version 3 and typed literals. Partition
transforms remain in Iceberg metadata rather than Hive partition keys.

Iceberg REST capabilities depend on the server's endpoints and format-version
support. Registration requires the register-table endpoint and server access to the
metadata file. `PURGE` forwards a deletion request to the server. Format upgrades,
defaults, and property removal have [server-specific limitations](./iceberg-rest#table-ddl).
Branch/tag rows include the `IF NOT EXISTS` and `CREATE OR REPLACE` variants.

### Catalog and Storage Requirements

Register an existing table using `CREATE TABLE ... USING delta|iceberg LOCATION '...'`
without a column list. The location must be accessible to Sail. Unity location
registration applies to external Delta tables; native Iceberg DDL uses an Iceberg
REST catalog connection instead of the Unity table API.

Memory and HMS drop table registrations without deleting the data, including when
`PURGE` is supplied. Glue and Unity reject `PURGE`. OneLake exposes read-only table
APIs, so Sail rejects table DDL before changing storage.

HMS publishes Iceberg metadata pointers and schema updates under a metastore lock;
Glue uses version-checked updates. Iceberg REST publishes schema and property
changes with REST commit requirements.
