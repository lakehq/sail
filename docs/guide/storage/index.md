---
title: Data Storage
rank: 5
---

# Data Storage

Sail supports various storage solutions, including file systems, cloud storage services, and other data sources.

You can use the `SparkSession.read` and `SparkSession.write` API to load data from and write data to the storage.

You can also use the `CREATE TABLE` SQL statement to create a table that refers to data stored in the storage.

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
| [HTTP/HTTPS](./http)                      | :white_check_mark: | :white_check_mark: |
| [HDFS](./hdfs)                            | :white_check_mark: | :white_check_mark: |
| [Hugging Face](./hf)                      | :white_check_mark: | :x:                |
| JDBC                                      | :x:                | :x:                |

## Supported URL Formats

Sail recognizes various URL formats to identify the appropriate storage backend:

- **File System**: `file:///path/to/file`
- **Memory**: `memory:///path`
- **S3**: `s3://bucket/path` or `s3a://bucket/path`
- **Azure**: `az://container/path`, `azure://container/path`, `abfs://container@account.dfs.core.windows.net/path`, `abfss://container@account.dfs.core.windows.net/path`
- **Google Cloud Storage**: `gs://bucket/path`
- **HTTP/HTTPS**: `http://example.com/path` or `https://example.com/path`
- **HDFS**: `hdfs://namenode:port/path`
- **Hugging Face**: `hf://datasets/org/dataset`

## Special URL Handling

Some HTTPS URLs are automatically recognized as cloud storage:

- **Azure**: URLs containing `dfs.core.windows.net`, `blob.core.windows.net`, `dfs.fabric.microsoft.com`, or `blob.fabric.microsoft.com`
- **S3**: URLs containing `amazonaws.com` or `r2.cloudflarestorage.com`

These URLs will use the appropriate cloud storage backend instead of the generic HTTP store.
