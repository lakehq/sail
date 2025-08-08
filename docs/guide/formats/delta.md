---
title: Delta Lake
rank: 1
---

# Delta Lake

You can use the `delta` format in Sail to work with Delta Lake.
You can use the Spark DataFrame API or Spark SQL to read and write Delta tables.

::: warning
The Delta Lake integration in Sail is under active development.
You can use Sail to read Delta tables and write new Delta tables.
But it is not recommended to use Sail to overwrite or modify existing Delta tables created by other engines.
If you encounter any issues or would like to request advanced Delta Lake features, feel free to reach out to us on [GitHub Issues](https://github.com/lakehq/sail/issues)!
:::

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
df.write.format("delta").mode("overwrite").save(path)
# This appends data to an existing Delta table.
df.write.format("delta").mode("append").save(path)

df = spark.read.format("delta").load(path)
df.show()
```

```sql [SQL]
CREATE TABLE users (id INT, name STRING)
USING delta
LOCATION 'file:///tmp/sail/users';

INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM users;
```

:::

### Data Partitioning

You can work with partitioned Delta tables using the Spark DataFrame API.
Partitioned Delta tables organize data into directories based on the values of one or more columns.
This improves query performance by skipping data files that do not match the filter conditions.

::: code-group

```python [Python]
path = "file:///tmp/sail/metrics"
df = spark.createDataFrame(
    [(2024, 1.0), (2025, 2.0)],
    schema="year INT, value FLOAT",
)

df.write.format("delta").mode("overwrite").partitionBy("year").save(path)

df = spark.read.format("delta").load(path).filter("year > 2024")
df.show()
```

```sql [SQL]
CREATE TABLE metrics (year INT, value FLOAT)
USING delta
LOCATION 'file:///tmp/sail/metrics'
PARTITIONED BY (year);

INSERT INTO metrics VALUES (2024, 1.0), (2025, 2.0);

SELECT * FROM metrics WHERE year > 2024;
```

:::

### Schema Evolution

Delta Lake handles schema evolution gracefully.
By default, if you try to write data with a different schema than the one of the existing Delta table, an error will occur.
You can enable schema evolution by setting the `mergeSchema` option to `true` when writing data.
In this case, if you change the data type of an existing column to a compatible type, or add a new column, Delta Lake will automatically update the schema of the table.

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```

You can also use the `overwriteSchema` option to overwrite the schema of an existing Delta table.
But this works only if you set the write mode to `overwrite`.

```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
```

### Time Travel

You can use the time travel feature of Delta Lake to query historical versions of a Delta table.

```python
df = spark.read.format("delta").option("versionAsOf", "0").load(path)
```

Time travel is not available for Spark SQL in Sail yet, but we plan to support it soon.

### More Features

We will continue adding more examples for advanced Delta Lake features as they become available in Sail.
In the meantime, feel free to reach out to us on [Slack](https://lakesail.com/slack) or [GitHub Discussions](https://github.com/lakehq/sail/discussions) if you have questions!
