---
title: PostgreSQL
rank: 2
---

# PostgreSQL

Sail includes a Python DataSource V2 implementation for reading PostgreSQL tables as Spark DataFrames.
It supports automatic schema inference, filter pushdown, and parallel partition-based reading.

## Installation

Install PySail with the PostgreSQL extra:

```bash-vue
pip install "pysail[postgres]=={{ libVersion }}"
```

This pulls in `psycopg2-binary` and `pyarrow` as additional dependencies.

## Quick Start

<!--@include: ../_common/spark-session.md-->

```python
from pysail.datasources.postgres import PostgresDataSource

spark.dataSource.register(PostgresDataSource)

df = spark.read.format("postgres").options(
    url="jdbc:postgresql://localhost:5432/mydb",
    user="myuser",
    password="mypassword",
    dbtable="users",
).load()

df.show()
```

## Parallel Reading

Split reads across multiple partitions using a numeric column.
Each partition reads rows where `MOD(partitionColumn, numPartitions) = partition_id`.

```python
df = spark.read.format("postgres").options(
    url="jdbc:postgresql://localhost:5432/mydb",
    user="myuser",
    password="mypassword",
    dbtable="large_table",
    numPartitions="4",
    partitionColumn="id",
).load()
```

## Filter Pushdown

Comparison filters (`=`, `>`, `>=`, `<`, `<=`) are automatically pushed down to PostgreSQL,
so only matching rows are transferred over the network.

```python
# This WHERE clause runs in PostgreSQL, not in Spark
df.filter("age > 25").show()
```

## Options

| Option            | Required | Default  | Description                                                  |
| ----------------- | -------- | -------- | ------------------------------------------------------------ |
| `url`             | Yes      |          | PostgreSQL JDBC URL (`jdbc:postgresql://host:port/database`) |
| `user`            | Yes      |          | Username                                                     |
| `password`        | Yes      |          | Password                                                     |
| `dbtable`         | Yes      |          | Table name                                                   |
| `tableSchema`     | No       | `public` | PostgreSQL schema containing the table                       |
| `numPartitions`   | No       | `1`      | Number of parallel readers (must be ≥ 1)                     |
| `partitionColumn` | No       |          | Column for partitioning (required when `numPartitions > 1`)  |
| `fetchsize`       | No       | `8192`   | Number of rows fetched per batch (must be ≥ 1)               |

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
