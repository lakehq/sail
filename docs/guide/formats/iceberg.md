---
title: Iceberg
rank: 2
---

# Iceberg

You can use the `iceberg` format in Sail to work with [Apache Iceberg](https://iceberg.apache.org/).
You can use the Spark DataFrame API or Spark SQL to read and write Iceberg tables.

## Examples

<!--@include: ../_common/spark-session.md-->

### Basic Usage

::: code-group

```python [Python]
path = "file:///tmp/sail/users"
df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob")],
    schema="id INT, name STRING",
)

# This creates a new table or overwrites an existing one.
df.write.format("iceberg").mode("overwrite").save(path)
# This appends data to an existing table.
df.write.format("iceberg").mode("append").save(path)

df = spark.read.format("iceberg").load(path)
df.show()
```

```sql [SQL]
CREATE TABLE users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/users';

INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM users;
```

:::

### Data Partitioning

You can work with partitioned Iceberg tables using the Spark DataFrame API.
Partitioned Iceberg tables organize data into directories based on the values of one or more columns.
This improves query performance by skipping data files that do not match the filter conditions.

::: code-group

```python [Python]
path = "file:///tmp/sail/metrics"
df = spark.createDataFrame(
    [(2024, 1.0), (2025, 2.0)],
    schema="year INT, value FLOAT",
)

df.write.format("iceberg").mode("overwrite").partitionBy("year").save(path)

df = spark.read.format("iceberg").load(path).filter("year > 2024")
df.show()
```

```sql [SQL]
CREATE TABLE metrics (year INT, value FLOAT)
USING iceberg
LOCATION 'file:///tmp/sail/metrics'
PARTITIONED BY (year);

INSERT INTO metrics VALUES (2024, 1.0), (2025, 2.0);

SELECT * FROM metrics WHERE year > 2024;
```

:::

### Time Travel

You can use the time travel feature to query branches or historical versions of an Iceberg table.

```python
df = spark.read.format("iceberg").option("snapshotId", "123").load(path)
df = spark.read.format("iceberg").option("timestampAsOf", "2025-01-02T03:04:05.678").load(path)
df = spark.read.format("iceberg").option("ref", "main").load(path)
```

Time travel is not available for Spark SQL in Sail yet, but we plan to support it soon.

### More Features

We will continue adding more examples for advanced Iceberg features as they become available in Sail.
In the meantime, feel free to reach out to us on [Slack](https://lakesail.com/slack) or [GitHub Discussions](https://github.com/lakehq/sail/discussions) if you have questions!
