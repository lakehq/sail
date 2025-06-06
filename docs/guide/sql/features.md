---
title: Supported Features
rank: 1
---

# Supported Features

This page presents a high-level overview of supported Spark SQL features.

## Literals

Sail supports all the Spark SQL literal syntax.

## Data Types

Sail supports all Spark SQL data types except the `VARIANT` type introduced in Spark 4.0. Support for the `VARIANT` type is tracked in the [GitHub issue](https://github.com/lakehq/sail/issues/511).

## Expressions

Sail supports most Spark SQL expression syntax, including unary and binary operators, predicates, `CASE` clause etc.

Sail supports most common Spark SQL functions. The effort to reach full function parity with Spark is tracked in the [GitHub issue](https://github.com/lakehq/sail/issues/398).

## Statements

### Data Retrieval (Query)

The following table lists the supported clauses in the `SELECT` statement.

| Clause                            | Supported          |
| --------------------------------- | ------------------ |
| `FROM <relation>`                 | :white_check_mark: |
| `FROM <format>.<path>` (files)    | :construction:     |
| `WHERE`                           | :white_check_mark: |
| `GROUP BY`                        | :white_check_mark: |
| `HAVING`                          | :white_check_mark: |
| `ORDER BY`                        | :white_check_mark: |
| `LIMIT`                           | :white_check_mark: |
| `OFFSET`                          | :white_check_mark: |
| `JOIN`                            | :white_check_mark: |
| `UNION`                           | :white_check_mark: |
| `INTERSECT`                       | :white_check_mark: |
| `EXCEPT` / `MINUS`                | :white_check_mark: |
| `WITH` (common table expressions) | :white_check_mark: |
| `VALUES` (inline tables)          | :white_check_mark: |
| `OVER <window>`                   | :white_check_mark: |
| `/*+ ... */` (hints)              | :construction:     |
| `SORT BY`                         | :construction:     |
| `CLUSTER BY`                      | :construction:     |
| `DISTRIBUTE BY`                   | :construction:     |
| `PIVOT`                           | :construction:     |
| `UNPIVOT`                         | :construction:     |
| `LATERAL VIEW`                    | :white_check_mark: |
| `LATERAL <subquery>`              | :construction:     |
| `TABLESAMPLE`                     | :construction:     |
| `TRANSFORM`                       | :construction:     |

The `EXPLAIN` statement is also supported, but the output shows the Sail logical and physical plan.

The `DESCRIBE QUERY` statement is not supported yet.

### Data Manipulation

| Statement                    | Supported      |
| ---------------------------- | -------------- |
| `INSERT TABLE`               | :construction: |
| `INSERT OVERWRITE DIRECTORY` | :construction: |
| `LOAD DATA`                  | :construction: |

### Catalog Management

| Statement            | Supported                    |
| -------------------- | ---------------------------- |
| `ALTER DATABASE`     | :construction:               |
| `ALTER TABLE`        | :construction:               |
| `ALTER VIEW`         | :construction:               |
| `ANALYZE TABLE`      | :construction:               |
| `CREATE DATABASE`    | :white_check_mark:           |
| `CREATE FUNCTION`    | :construction:               |
| `CREATE TABLE`       | :white_check_mark: (partial) |
| `CREATE VIEW`        | :white_check_mark: (partial) |
| `DESCRIBE DATABASE`  | :construction:               |
| `DESCRIBE FUNCTION`  | :construction:               |
| `DESCRIBE TABLE`     | :construction:               |
| `DROP DATABASE`      | :white_check_mark:           |
| `DROP FUNCTION`      | :construction:               |
| `DROP TABLE`         | :white_check_mark:           |
| `DROP VIEW`          | :white_check_mark:           |
| `REFRESH <path>`     | :construction:               |
| `REFRESH TABLE`      | :construction:               |
| `REFRESH FUNCTION`   | :construction:               |
| `REPAIR TABLE`       | :construction:               |
| `SHOW COLUMNS`       | :construction:               |
| `SHOW CREATE TABLE`  | :construction:               |
| `SHOW DATABASES`     | :construction:               |
| `SHOW FUNCTIONS`     | :construction:               |
| `SHOW PARTITIONS`    | :construction:               |
| `SHOW TABLE`         | :construction:               |
| `SHOW TABLES`        | :construction:               |
| `SHOW TBLPROPERTIES` | :construction:               |
| `SHOW VIEWS`         | :construction:               |
| `TRUNCATE TABLE`     | :construction:               |
| `USE DATABASE`       | :white_check_mark:           |

Currently, Sail only supports in-memory catalog, which means the databases and tables are available only within the session.
Remote catalog support is in our roadmap.

### Configuration Management

| Statement                   | Supported      |
| --------------------------- | -------------- |
| `RESET` (reset options)     | :construction: |
| `SET` (list or set options) | :construction: |

### Artifact Management

| Statement   | Supported |
| ----------- | --------- |
| `ADD FILE`  | :x:       |
| `ADD JAR`   | :x:       |
| `LIST FILE` | :x:       |
| `LIST JAR`  | :x:       |

### Cache Management

| Statement       | Supported      |
| --------------- | -------------- |
| `CACHE TABLE`   | :construction: |
| `CLEAR CACHE`   | :construction: |
| `UNCACHE TABLE` | :construction: |
