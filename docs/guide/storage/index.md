---
title: Data Storage
rank: 6
---

# Data Storage

Sail provides a unified interface for reading and writing data across various storage systems, from local file systems to cloud object stores and distributed file systems. This abstraction allows you to seamlessly work with data regardless of where it's stored, using the same familiar Spark APIs.

## Quick Examples

<!--@include: ../_common/spark-session.md-->

```python
# Local file system
df = spark.read.parquet("/path/to/local/data")
df = spark.read.parquet("file:///path/to/local/data")

# Cloud storage
df = spark.read.parquet("s3://bucket/data")
df = spark.read.parquet("azure://container/data")
df = spark.read.parquet("gs://bucket/data")

# In-memory storage
df = spark.read.parquet("memory:///cached/data")

# HTTP/HTTPS endpoints
df = spark.read.json("https://api.example.com/data.json")

# Create tables from any storage
spark.sql("""
CREATE TABLE my_table (id INT, name STRING)
USING parquet
LOCATION 's3://bucket/path/to/data'
""")
```

## Storage Support Matrix

Here is a summary of the supported (:white_check_mark:) and unsupported (:x:) storage features for reading and writing data. There are also features that are planned in our roadmap (:construction:).

| Storage                                   | Read Support       | Write Support      |
| ----------------------------------------- | ------------------ | ------------------ |
| [File Systems](./fs)                      | :white_check_mark: | :white_check_mark: |
| [Memory](./memory)                        | :white_check_mark: | :white_check_mark: |
| [AWS S3](./s3)                            | :white_check_mark: | :white_check_mark: |
| [Cloudflare R2](./s3)                     | :white_check_mark: | :white_check_mark: |
| [Azure Data Lake Storage (ADLS)](./azure) | :white_check_mark: | :white_check_mark: |
| [Azure Blob Storage](./azure)             | :white_check_mark: | :white_check_mark: |
| [Google Cloud Storage](./gcs)             | :white_check_mark: | :white_check_mark: |
| [HDFS](./hdfs)                            | :white_check_mark: | :white_check_mark: |
| [Hugging Face](./hf)                      | :white_check_mark: | :x:                |
| HTTP/HTTPS                                | :white_check_mark: | :white_check_mark: |
| JDBC                                      | :x:                | :x:                |

## Special URL Handling

Some HTTPS URLs are automatically recognized as cloud storage:

- **S3**: URLs containing `amazonaws.com` or `r2.cloudflarestorage.com`.
- **Azure**: URLs containing `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, or `blob.fabric.microsoft.com`.

These URLs will use the appropriate cloud storage backend instead of the generic HTTP store.
