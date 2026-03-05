---
title: Supported Features
rank: 2
---

# Supported Features

## Overview

Here is a high-level overview of the features supported by Sail for Iceberg tables.

| Feature           | Supported          |
| ----------------- | ------------------ |
| Read              | :white_check_mark: |
| Write (append)    | :white_check_mark: |
| Write (overwrite) | :white_check_mark: |
| `DELETE`          | :white_check_mark: |
| `MERGE`           | :construction:     |
| `UPDATE`          | :construction:     |

Both non-partitioned and partitioned tables are supported for reading and writing.

The write operations currently follow "copy-on-write" semantics.
We plan to support delete files and deletion vectors, which would enable "merge-on-read" write operations in the future.

## Version-specific Features

We classify the supported features according to the [Iceberg specification](https://iceberg.apache.org/spec/).

### Version 1: Analytic Data Tables

| Feature               | Supported          |
| --------------------- | ------------------ |
| Metadata              | :white_check_mark: |
| Manifest list         | :white_check_mark: |
| File format (Parquet) | :white_check_mark: |
| File format (Avro)    | :white_check_mark: |
| File format (ORC)     | :construction:     |
| Schema evolution      | :white_check_mark: |
| Partition evolution   | :construction:     |
| Time travel           | :white_check_mark: |
| Column statistics     | :white_check_mark: |

Reading existing branches and tags is supported (time travel).
We plan to support creating branches and tags in DDL operations in the future.

### Version 2: Row-Level Deletes

| Feature             | Supported          |
| ------------------- | ------------------ |
| Delete files        | :construction:     |
| Sequence numbers    | :white_check_mark: |
| Manifest extensions | :construction:     |

### Version 3: Extended Types and Capabilities

| Feature               | Supported      |
| --------------------- | -------------- |
| Deletion vectors      | :construction: |
| Row lineage           | :construction: |
| Column default values | :construction: |
| Encryption keys       | :construction: |
