---
title: Supported Features
rank: 2
---

# Supported Features

## Overview

The following table lists the features supported by Sail for Iceberg tables.

| Feature                               | Supported          |
| ------------------------------------- | ------------------ |
| Current snapshot reads                | :white_check_mark: |
| Metadata-as-data read path            | :white_check_mark: |
| Append writes                         | :white_check_mark: |
| Overwrite writes                      | :white_check_mark: |
| `CREATE OR REPLACE` / `REPLACE TABLE` | :white_check_mark: |
| Predicate overwrite writes            | :construction:     |
| Partition overwrite writes            | :construction:     |
| Copy-on-write writes                  | :white_check_mark: |
| Merge-on-read writes                  | :construction:     |
| Row-level `DELETE`                    | :construction:     |
| `MERGE INTO`                          | :construction:     |
| `UPDATE`                              | :construction:     |
| Partitioned tables                    | :white_check_mark: |
| Predicate pushdown and file pruning   | :white_check_mark: |
| Time travel (by snapshot ID)          | :white_check_mark: |
| Time travel (by timestamp)            | :white_check_mark: |
| Snapshot references (`refs`)          | :white_check_mark: |
| Branch and tag creation               | :construction:     |
| Table property DDL                    | :white_check_mark: |
| Format-version selection              | :white_check_mark: |
| Commit conflict resolution and retry  | :construction:     |
| Iceberg REST catalog integration      | :white_check_mark: |
| File system table commits             | :white_check_mark: |
| Multi-table atomic transactions       | :construction:     |
| Compaction (`rewrite_data_files`)     | :construction:     |
| Position-delete rewrite procedures    | :construction:     |
| Z-order clustering                    | :construction:     |
| Iceberg View Spec                     | :construction:     |
| Iceberg SQL UDF Spec                  | :construction:     |

Both non-partitioned and partitioned tables are supported for reading and writing.

The write operations currently follow "copy-on-write" semantics.
We plan to support delete files and deletion vectors, which would enable "merge-on-read" write operations in the future.

## Version-specific Features

We classify the supported features according to the [Iceberg specification](https://iceberg.apache.org/spec/).

### Version 1: Analytic Data Tables

| Feature                          | Supported          |
| -------------------------------- | ------------------ |
| Table metadata                   | :white_check_mark: |
| Schemas and Data Types           | :white_check_mark: |
| Schema Evolution                 | :white_check_mark: |
| Partition specs                  | :white_check_mark: |
| Partition transforms             | :white_check_mark: |
| Partition Evolution              | :construction:     |
| Sort Orders                      | :construction:     |
| Snapshots                        | :white_check_mark: |
| Manifest Lists                   | :white_check_mark: |
| Manifests                        | :white_check_mark: |
| Scan Planning                    | :white_check_mark: |
| File format (Parquet data files) | :white_check_mark: |
| File format (Avro data files)    | :construction:     |
| File format (ORC data files)     | :construction:     |
| Column metrics                   | :white_check_mark: |
| NaN value counts                 | :construction:     |
| Name Mapping                     | :construction:     |
| Table Statistics                 | :construction:     |
| Partition Statistics             | :construction:     |
| Snapshot expiration              | :construction:     |

Reading existing branches and tags is supported (time travel).
We plan to support creating branches and tags in DDL operations in the future.

### Version 2: Row-Level Deletes

| Feature                        | Supported          |
| ------------------------------ | ------------------ |
| Format-version 2 metadata      | :white_check_mark: |
| Sequence Numbers               | :white_check_mark: |
| Sequence Number Inheritance    | :white_check_mark: |
| Manifest Lists sequence fields | :white_check_mark: |
| Manifest `content` metadata    | :construction:     |
| Data file `content` field      | :construction:     |
| Delete Formats                 | :construction:     |
| Position Delete Files          | :construction:     |
| Equality Delete Files          | :construction:     |
| Delete File Stats              | :construction:     |
| Row-level delete scan planning | :construction:     |
| Row-level delete writes        | :construction:     |

### Version 3: Extended Types and Capabilities

| Feature                      | Supported          |
| ---------------------------- | ------------------ |
| Variant                      | :white_check_mark: |
| Unknown type                 | :construction:     |
| `timestamp_ns`               | :construction:     |
| `timestamptz_ns`             | :construction:     |
| Geometry type                | :construction:     |
| Geography type               | :construction:     |
| Default Values               | :construction:     |
| Multi-argument transforms    | :construction:     |
| Row Lineage                  | :construction:     |
| First Row ID Inheritance     | :white_check_mark: |
| Deletion Vectors             | :construction:     |
| Encryption Keys              | :construction:     |
| AES-GCM stream encryption    | :construction:     |
| Puffin deletion-vector blobs | :construction:     |
