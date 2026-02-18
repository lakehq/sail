---
title: JDBC
rank: 3
---

# JDBC Datasource

Sail provides a database connector exposed under the `"jdbc"` format name for API parity with
vanilla PySpark — no actual JDBC driver or JVM is involved.

## Installation

```bash
pip install pysail[jdbc]
```

## Quick Start

Register the datasource once per Spark session, then read using the standard PySpark API.

```python
from pysail.datasources.jdbc import JdbcDataSource

spark.dataSource.register(JdbcDataSource)

# Using format("jdbc") — full option control
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/mydb")
    .option("dbtable", "public.users")
    .option("user", "alice")
    .option("password", "secret")
    .load()
)

# Using spark.read.jdbc() shorthand
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/mydb",
    "public.users",
    properties={"user": "alice", "password": "secret"},
)
df.show()
```

## Supported Options

Options are consistent with the [PySpark JDBC documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).

| Option              | Required  | Default | Description                                                                                         |
| ------------------- | --------- | ------- | --------------------------------------------------------------------------------------------------- |
| `url`               | **Yes**   |         | JDBC URL: `jdbc:<subprotocol>://<host>:<port>/<db>`                                                 |
| `dbtable`           | **Yes\*** |         | Table name, optionally schema-qualified (`"schema.table"`). Mutually exclusive with `query`.        |
| `query`             | **Yes\*** |         | Arbitrary SQL SELECT. Mutually exclusive with `dbtable`.                                            |
| `user`              | No        |         | Database username. Can also be passed in `properties` dict.                                         |
| `password`          | No        |         | Database password. Can also be passed in `properties` dict.                                         |
| `partitionColumn`   | No        |         | Numeric column for range-stride partitioning. Requires `lowerBound`, `upperBound`, `numPartitions`. |
| `lowerBound`        | No        |         | Lower bound of partition stride (inclusive).                                                        |
| `upperBound`        | No        |         | Upper bound of partition stride (inclusive on last partition).                                      |
| `numPartitions`     | No        | `1`     | Number of parallel read partitions.                                                                 |
| `fetchsize`         | No        | `0`     | Advisory rows-per-round-trip hint.                                                                  |
| `pushDownPredicate` | No        | `true`  | Push `WHERE` filters to the database. Set to `false` to disable.                                    |
| `customSchema`      | No        |         | Spark DDL string to override inferred column types (e.g. `"id DECIMAL(38,0), name STRING"`).        |

\* Exactly one of `dbtable` or `query` is required.

## Reading a Custom SQL Query

Use `query` instead of `dbtable` to run arbitrary SQL:

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

> **Note:** `query` and `partitionColumn` are mutually exclusive. To partition
> a custom query, wrap it in `dbtable` as a subquery:
>
> ```python
> .option("dbtable", "(SELECT * FROM events WHERE type='click') AS t")
> .option("partitionColumn", "user_id")
> ```

## Parallel Reads (Range Partitioning)

Provide `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` together to
split the read into parallel range strides — consistent with PySpark JDBC semantics:

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

Or equivalently:

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

## Schema Override

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
