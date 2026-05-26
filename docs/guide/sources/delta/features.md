---
title: Supported Features
rank: 2
---

# Supported Features

## Core Table Operations

| Feature                                     | Supported          |
| ------------------------------------------- | ------------------ |
| Table snapshot reads                        | :white_check_mark: |
| Append writes                               | :white_check_mark: |
| Overwrite writes                            | :white_check_mark: |
| Conditional overwrite (`REPLACE WHERE`)     | :white_check_mark: |
| Partition value serialization               | :white_check_mark: |
| Data skipping (partition pruning)           | :white_check_mark: |
| Data skipping (pruning via file statistics) | :white_check_mark: |
| Schema validation                           | :white_check_mark: |
| Schema evolution                            | :white_check_mark: |
| Type widening                               | :white_check_mark: |
| Time travel (by version)                    | :white_check_mark: |
| Time travel (by timestamp)                  | :white_check_mark: |

Both non-partitioned and partitioned tables are supported for reading and writing.

## DML Operations

| Feature                                | Supported          |
| -------------------------------------- | ------------------ |
| `DELETE` (copy-on-write)               | :white_check_mark: |
| `DELETE` (deletion vectors)            | :white_check_mark: |
| `MERGE INTO` (copy-on-write)           | :white_check_mark: |
| `MERGE INTO` (deletion-vector deletes) | :white_check_mark: |
| `MERGE INTO` (deletion-vector updates) | :construction:     |
| `UPDATE`                               | :construction:     |

The "merge-on-read" mode refers to updating the table with deletion vectors. This reduces the amount of data that needs to be rewritten during DML operations, but incurs additional read overhead when querying the table.

## Table Maintenance Operations

| Feature    | Supported      |
| ---------- | -------------- |
| `VACUUM`   | :construction: |
| `OPTIMIZE` | :construction: |
| `RESTORE`  | :construction: |

## Protocol Internals

| Feature                        | Supported          |
| ------------------------------ | ------------------ |
| Data files                     | :white_check_mark: |
| Delta log entries              | :white_check_mark: |
| `protocol` action              | :white_check_mark: |
| `metaData` action              | :white_check_mark: |
| `add` action                   | :white_check_mark: |
| `remove` action                | :white_check_mark: |
| `txn` action                   | :white_check_mark: |
| `commitInfo` action            | :white_check_mark: |
| `cdc` action                   | :construction:     |
| Domain Metadata action         | :construction:     |
| Classic checkpoint             | :white_check_mark: |
| UUID-named V2 checkpoint       | :white_check_mark: |
| Checkpoint sidecar files       | :white_check_mark: |
| Multi-part checkpoint          | :x:                |
| `stats_parsed`                 | :white_check_mark: |
| `partitionValues_parsed`       | :white_check_mark: |
| Log compaction files           | :white_check_mark: |
| Last checkpoint file           | :white_check_mark: |
| Version checksum file          | :white_check_mark: |
| Append-only Tables             | :white_check_mark: |
| Column Mapping                 | :white_check_mark: |
| Deletion Vectors               | :white_check_mark: |
| Timestamp without Timezone     | :white_check_mark: |
| V2 Checkpoint table feature    | :white_check_mark: |
| In-Commit Timestamps           | :white_check_mark: |
| Generated Columns              | :white_check_mark: |
| Variant Data Type              | :white_check_mark: |
| Type Widening                  | :white_check_mark: |
| Column Invariants              | :construction:     |
| `CHECK` Constraints            | :white_check_mark: |
| Default Columns                | :construction:     |
| Identity Columns               | :construction:     |
| Change Data Feed               | :construction:     |
| Row Tracking                   | :construction:     |
| Catalog-managed Tables         | :construction:     |
| Iceberg Compatibility V1       | :construction:     |
| Iceberg Compatibility V2       | :construction:     |
| Clustered Table                | :construction:     |
| VACUUM Protocol Check          | :construction:     |
| Transaction conflict detection | :construction:     |
| Unity Catalog integration      | :white_check_mark: |
| AWS Glue / Hive Metastore      | :construction:     |
| Streaming reads                | :construction:     |
| Streaming writes               | :construction:     |

`CHECK` constraint support covers stored `delta.constraints.*` expressions on
Delta write and `MERGE` paths. Validating existing rows when adding constraints
with `ALTER TABLE SET TBLPROPERTIES` is not yet supported.
