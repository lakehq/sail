# PostgreSQL DataSource for PySpark

A Python DataSource V2 implementation that reads PostgreSQL tables as Spark DataFrames, with automatic schema inference, filter pushdown, and parallel partition-based reading.

## Installation

Install pysail with PostgreSQL support:

```bash
pip install pysail[postgres]
```

This will install the required dependencies: `psycopg2-binary` and `pyarrow`.

## Development Setup

For local development and testing:

```bash
# 1. Start PostgreSQL test database (from repo root)
docker compose --profile datasources up -d

# 2. Build pysail in development mode with postgres extras
hatch run maturin develop --extras postgres

# 3. Run tests
hatch run python python/pysail/datasources/postgres/test_postgres_datasource.py
```

The PostgreSQL container includes sample test data (users, products, orders tables) loaded from `init.sql`.

## Quick Start

```python
from pyspark.sql import SparkSession
from pysail.datasources.postgres import PostgresDataSource

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

spark.dataSource.register(PostgresDataSource)

df = spark.read.format("postgres").options(
    host="localhost",
    port="5432",
    database="mydb",
    user="myuser",
    password="mypassword",
    table="users",
).load()

df.show()
```

## Parallel Reading

Split reads across multiple partitions using a numeric column:

```python
df = spark.read.format("postgres").options(
    database="mydb",
    user="myuser",
    password="mypassword",
    table="large_table",
    numPartitions="4",
    partitionColumn="id",
).load()
```

Each partition reads rows where `MOD(partitionColumn, numPartitions) = partition_id`.

## Filter Pushdown

Comparison filters (`=`, `>`, `>=`, `<`, `<=`) are automatically pushed down to PostgreSQL, so only matching rows are transferred.

```python
# This WHERE clause runs in PostgreSQL, not Spark
df.filter("age > 25").show()
```

## Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `database` | Yes | | Database name |
| `user` | Yes | | Username |
| `password` | Yes | | Password |
| `table` | Yes | | Table to read |
| `host` | No | `localhost` | Server hostname |
| `port` | No | `5432` | Server port |
| `numPartitions` | No | `1` | Number of parallel readers |
| `partitionColumn` | No | | Column for partitioning (required when `numPartitions > 1`) |
| `batchSize` | No | `8192` | Number of rows to fetch per batch |
