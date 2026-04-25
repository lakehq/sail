---
name: sail-datasource
description: Expert guidance for data sources and table formats in Sail. Use when implementing new data connectors, working with Delta Lake or Iceberg, or optimizing storage performance. Triggers include adding new file formats, configuring storage backends (S3, Azure, GCS), implementing catalog providers, or optimizing partitioning and data skipping.
---

# DataSource & Table Formats

Sail supports multiple file formats, storage backends, and table formats.

## Supported Formats

| Format | Read | Write |
|--------|------|-------|
| Parquet | ✓ | ✓ |
| CSV | ✓ | ✓ |
| JSON | ✓ | ✓ |
| Delta Lake | ✓ | ✓ (partial) |
| Iceberg | ✓ | ✓ (partial) |

## Storage Backends

```python
# AWS S3
df = spark.read.parquet("s3://bucket/path")

# Azure Blob Storage
df = spark.read.parquet("abfs://container@account/path")

# GCS
df = spark.read.parquet("gs://bucket/path")

# HDFS
df = spark.read.parquet("hdfs://namenode:8020/path")
```

## Delta Lake

```python
# Read Delta Lake
df = spark.read.format("delta").load("s3://bucket/delta_table")

# Time travel
df = spark.read.format("delta").option("versionAsOf", "1").load(...)
```

## Iceberg

```python
# Via catalog
spark.conf.set("spark.sql.catalog.catalog", "rest")
spark.conf.set("spark.sql.catalog.catalog.uri", "http://catalog:8181")
df = spark.table("catalog.db.table")
```

## Resources

- **[datasource.md](references/datasource.md)**: Comprehensive guide for data sources and formats
- **[distributed.md](../sail-distributed/references/distributed.md)**: Distributed data shuffling
