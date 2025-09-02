---
title: Google Cloud Storage
rank: 6
---

# Google Cloud Storage

Sail supports reading from and writing to Google Cloud Storage (GCS) buckets.

## URI Format

Google Cloud Storage uses the `gs://` URI scheme.

## Configuration

You can use environment variables to configure GCS in Sail.
Some configuration options can be set using different environment variables.

::: warning
The environment variables to configure GCS are experimental and may change in future versions of Sail.
:::

- `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`

  The path to the service account file used for authentication.

- `GOOGLE_SERVICE_ACCOUNT_KEY`

  The serialized service account key.

- `GOOGLE_APPLICATION_CREDENTIALS`

  The path to the application credentials file.

- `GOOGLE_SKIP_SIGNATURE`

  Whether to skip signing requests. This is useful for public buckets.

::: info
For configuration options that accept boolean values, you can specify `1`, `true`, `on`, `yes`, or `y` for a true value, and specify `0`, `false`, `off`, `no`, or `n` for a false value.
The boolean values are case-insensitive.
:::

## Examples

<!--@include: ../_common/spark-session.md-->

### Spark DataFrame API

```python
path = "gs://my-bucket/path/to/data"

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema="id INT, name STRING")
df.write.parquet(path)

df = spark.read.parquet(path)
df.show()
```

### Spark SQL

```python
sql = """
CREATE TABLE my_table (id INT, name STRING)
USING parquet
LOCATION 'gs://my-bucket/path/to/data'
"""
spark.sql(sql)
spark.sql("SELECT * FROM my_table").show()

spark.sql("INSERT INTO my_table VALUES (3, 'Charlie'), (4, 'David')")
spark.sql("SELECT * FROM my_table").show()
```
