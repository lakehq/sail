---
title: Data Storage
rank: 5
---

# Data Storage

Sail provides a unified interface for reading and writing data across various storage systems, from local file systems to cloud object stores and distributed file systems. This abstraction allows you to seamlessly work with data regardless of where it's stored, using the same familiar Spark APIs.

## Overview

The storage layer in Sail provides:

- **Unified API**: Use the same `spark.read` and `spark.write` operations across all storage types
- **Automatic Detection**: Sail automatically determines the storage backend based on URL format
- **High Performance**: Optimized connectors for each storage system
- **Seamless Integration**: Works with Spark SQL, DataFrames, and streaming APIs

## Quick Examples

```python
# Local file system
df = spark.read.parquet("/path/to/local/data")

# Cloud storage
df = spark.read.parquet("s3://bucket/data")
df = spark.read.parquet("gs://bucket/data")
df = spark.read.parquet("azure://container/data")

# In-memory storage
df = spark.read.parquet("memory:///cached/data")

# HTTP endpoints
df = spark.read.json("https://api.example.com/data.json")

# Create tables from any storage
spark.sql("""
    CREATE TABLE my_table
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
| [Cloudflare R2](./s3#cloudflare-r2)       | :white_check_mark: | :white_check_mark: |
| [Azure Data Lake Storage (ADLS)](./azure) | :white_check_mark: | :white_check_mark: |
| [Azure Blob Storage](./azure)             | :white_check_mark: | :white_check_mark: |
| [Google Cloud Storage](./gcs)             | :white_check_mark: | :white_check_mark: |
| [HDFS](./hdfs)                            | :white_check_mark: | :white_check_mark: |
| [Hugging Face](./hf)                      | :white_check_mark: | :x:                |
| HTTP/HTTPS                                | :white_check_mark: | :white_check_mark: |
| JDBC                                      | :x:                | :x:                |

## Supported URL Formats

Sail recognizes various URL formats to identify the appropriate storage backend:

- **File System**: `file:///path/to/file`
- **Memory**: `memory:///path`
- **S3**: `s3://bucket/path` or `s3a://bucket/path`
- **Cloudflare R2**: `"https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/path"`
- **Azure**: `az://container/path`, `azure://container/path`, `abfs://container/path`, `abfss://container/path`, `adl://container/path`
- **Google Cloud Storage**: `gs://bucket/path`
- **HTTP/HTTPS**: `http://example.com/path` or `https://example.com/path`
- **HDFS**: `hdfs://namenode:port/path`
- **Hugging Face**: `hf://datasets/org/dataset`

## Special URL Handling

Some HTTPS URLs are automatically recognized as cloud storage:

- **Azure**: URLs containing `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, or `blob.fabric.microsoft.com`
- **S3**: URLs containing `amazonaws.com` or `r2.cloudflarestorage.com`

These URLs will use the appropriate cloud storage backend instead of the generic HTTP store.
