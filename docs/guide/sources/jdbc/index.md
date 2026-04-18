---
title: JDBC
rank: 4
---

# JDBC Data Source

Sail provides a database connector exposed under the `jdbc` format name for API parity with vanilla PySpark.
The implementation is based on the Python `connectorx` library.
No actual JDBC driver or JVM is involved.

<!--@include: ../../_common/spark-session.md-->

## Installation

You need to install the `pysail` package with the `jdbc` extra to use the JDBC data source.

```bash
pip install pysail[jdbc]
```

## Quick Start

Register the datasource once per Spark session.

```python
from pysail.spark.datasource.jdbc import JdbcDataSource

spark.dataSource.register(JdbcDataSource)
```

Then read from a database using the standard PySpark API.

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "public.users")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Alternatively, you can use the `spark.read.jdbc()` shorthand method.

```python
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/mydb",
    "public.users",
    properties={"user": "alice", "password": "secret"},
)
```

## Options

The data source options are consistent with
the [PySpark JDBC documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).

| Name                | Required | Default | Description                                                                                                      |
| ------------------- | -------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| `url`               | Yes      |         | The JDBC URL in the form of `jdbc:<subprotocol>://<host>:<port>/<db>`.                                           |
| `dbtable`           | Yes      |         | The table name, optionally qualified (`<schema>.<table>`). This is mutually exclusive with `query`.              |
| `query`             | Yes      |         | An arbitrary SQL `SELECT` statement. This is mutually exclusive with `dbtable`.                                  |
| `user`              | No       |         | The database username.                                                                                           |
| `password`          | No       |         | The database password.                                                                                           |
| `partitionColumn`   | No       |         | The numeric column for range-stride partitioning. This requires `lowerBound`, `upperBound`, and `numPartitions`. |
| `lowerBound`        | No       |         | The lower bound of partition stride (inclusive).                                                                 |
| `upperBound`        | No       |         | The upper bound of partition stride (inclusive on last partition).                                               |
| `numPartitions`     | No       | `1`     | The number of parallel read partitions.                                                                          |
| `fetchsize`         | No       | `0`     | An advisory rows-per-round-trip hint.                                                                            |
| `pushDownPredicate` | No       | `true`  | Whether to push `WHERE` filters to the database.                                                                 |
| `customSchema`      | No       |         | A Spark DDL string to override inferred column types.                                                            |

::: info
Exactly one of the `dbtable` or `query` options is required.
:::

## Examples

### Custom SQL Queries

Use `query` instead of `dbtable` to run arbitrary SQL queries:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("query", "SELECT id, name FROM users WHERE active = TRUE")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

The `query` and `partitionColumn` options are mutually exclusive. To partition a custom query, wrap it in `dbtable` as a
subquery:

```python{4-5}
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "(SELECT * FROM events WHERE type='click') AS t")
    .option("partitionColumn", "user_id")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

### Parallel Reads with Range Partitioning

Provide `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` together to split the read into parallel
range strides:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "events")
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "8")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Or equivalently, you can use the `spark.read.jdbc()` method with the same options:

```python
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/mydb",
    "events",
    column="id",
    lowerBound=1,
    upperBound=10_000_000,
    numPartitions=8,
    properties={"user": "alice", "password": "secret"},
)
```

### Schema Override

Use `customSchema` to override column types after reading:

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "orders")
    .option("customSchema", "amount DECIMAL(18,2), status STRING")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)
```

Columns not listed in `customSchema` retain their inferred types.
