---
title: Storage
rank: 5
---

# Storage

Sail supports accessing data from various sources, including local files and cloud storage services.
You can use the `SparkSession.read` and `SparkSession.write` API with the following types of paths.

Relative file paths

: These are file paths relative to the current working directory, such as `path/data.json`.

`file://` URIs

: These are absolute file paths on the local file system, such as `file:///path/to/file`.

`s3://` URIs

: These are paths in AWS S3 or an S3-compatible object storage, such as `s3://bucket/path/to/data`.

`hdfs://` URIs

: These are paths in HDFS, such as `hdfs://namenode:port/path/to/data`.

`hf://` URIs

: These are paths for Hugging Face datasets, such as `hf://datasets/username/dataset@~parquet/train`.

::: info

- For local file systems, the path can refer to a file or a directory.
- For S3-compatible object storage services, the path can refer to an object or a key prefix.
  We assume the key prefix is followed by `/` and represents a directory.

:::
