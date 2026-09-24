---
title: Supported Features
rank: 2
---

# Supported Features

This page lists Iceberg features supported in Sail.
:white_check_mark: means supported, **Partial** means supported for the cases listed, and :x: means not supported.

## Format Versions

New Iceberg tables created in Sail default to **version 2**. Use the `format-version` table property to select a version at creation or upgrade an existing table.

| Feature                           | Supported          | Notes                                                                        |
| --------------------------------- | ------------------ | ---------------------------------------------------------------------------- |
| Format versions 1, 2, and 3       | :white_check_mark: | Reads and writes table metadata; version-specific features are listed below. |
| Format-version upgrades           | :white_check_mark: | Through the `format-version` table property.                                 |
| Format-version downgrades         | :x:                | —                                                                            |
| Parquet data and delete files     | :white_check_mark: | Delete-file support is listed by version below.                              |
| Avro manifests and manifest lists | :white_check_mark: | —                                                                            |
| Avro and ORC data files           | :x:                | —                                                                            |

## Core Table Operations

| Feature                                            | Supported          | Notes                                                                                                                                                                                                       |
| -------------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table creation, CTAS, and replacement              | :white_check_mark: | Through SQL and the DataFrame writer APIs.                                                                                                                                                                  |
| Current snapshot reads, append, and full overwrite | :white_check_mark: | Both partitioned and non-partitioned tables.                                                                                                                                                                |
| Metadata-as-data reads                             | :white_check_mark: | Optional manifest scanning during query execution instead of loading the file list on the driver.                                                                                                           |
| Predicate pushdown and file pruning                | :white_check_mark: | Uses partition transforms and available file metrics.                                                                                                                                                       |
| Metadata aggregate optimization                    | :white_check_mark: | Eligible `COUNT`, `MIN`, and `MAX` queries use exact metadata; incomplete metrics or applicable deletes can require scanning data.                                                                          |
| Schema evolution on write                          | :white_check_mark: | `mergeSchema` adds fields and applies supported promotions; `overwriteSchema` replaces the schema during full overwrite. The two options cannot be combined.                                                |
| Predicate overwrite                                | Partial            | `DataFrameWriterV2.overwrite(condition)` on identity-partition columns; requires compatible live partition specs and no active delete files.                                                                |
| Dynamic partition overwrite                        | Partial            | `overwritePartitions()` or `overwrite-mode = dynamic` on existing tables with compatible live partition specs and no active delete files. Replaces partitions present in the input; empty input is a no-op. |
| Time travel                                        | :white_check_mark: | Snapshot ID, timestamp, or an existing branch/tag reference; requires the referenced metadata and data files.                                                                                               |
| Branch/tag creation and branch writes              | :x:                | —                                                                                                                                                                                                           |
| Table property DDL                                 | Partial            | Filesystem-backed tables support `SET/UNSET TBLPROPERTIES`, including format upgrades. Catalog-managed metadata `ALTER TABLE` is not supported.                                                             |
| Commit conflict handling                           | Partial            | Validates metadata requirements and the expected snapshot, with limited metadata publication retries. Row-level conflicts require replanning, including with `snapshot` isolation.                          |

## DML Operations

Iceberg uses copy-on-write to rewrite affected data files and merge-on-read to record deletes separately from data files.
In Sail, copy-on-write is the default for `DELETE`, `UPDATE`, and `MERGE INTO`.
The `write.delete.mode`, `write.update.mode`, and `write.merge.mode` table properties select the mode for each operation.

| Operation                                       | Copy-on-write                   | Merge-on-read                                                                     | Notes                                                                                                                                         |
| ----------------------------------------------- | ------------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `DELETE`                                        | :white_check_mark: Versions 1–3 | Partial: equality-delete files on unpartitioned version 2 or 3 tables.            | Merge-on-read uses all columns as equality keys; nested, floating-point, `unknown`, and `variant` fields are unsupported in this writer path. |
| `UPDATE`                                        | :white_check_mark: Versions 1–3 | :x:                                                                               | —                                                                                                                                             |
| `MERGE INTO` with inserts, updates, and deletes | :white_check_mark: Versions 1–3 | Partial: position-delete files on version 2 tables, including partitioned tables. | Matched, insert, and `WHEN NOT MATCHED BY SOURCE` clauses. Multiple source matches cannot update one target row.                              |
| `MERGE WITH SCHEMA EVOLUTION`                   | :x:                             | :x:                                                                               | —                                                                                                                                             |

Whole-file deletes proven from metadata remove file references directly, including on partitioned tables.
Merge-on-read `MERGE` appends replacement data for updated rows.

## Metadata, Schema, and Layout

The following metadata and layout features apply across supported format versions.

| Feature                                                  | Supported          | Notes                                                                                                                                             |
| -------------------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table metadata, snapshots, manifest lists, and manifests | :white_check_mark: | Reads and writes the supported version-specific fields.                                                                                           |
| Field IDs and schema history                             | :white_check_mark: | Resolves evolved columns by ID, including nested fields.                                                                                          |
| Partition transforms                                     | :white_check_mark: | Identity, bucket, truncate, year, month, day, and hour; existing void transforms are handled.                                                     |
| Partition evolution                                      | Partial            | Reads existing specs and writes using the current spec; schema-replacing overwrite can change partitioning. No partition-evolution DDL.           |
| Sort orders                                              | Partial            | Honors supported existing single-source sort transforms and records sort-order IDs on data files. No sort-order DDL or multi-argument transforms. |
| Column metrics                                           | :white_check_mark: | Uses file counts, null counts, bounds, and other available metrics for planning.                                                                  |
| NaN value counts                                         | Partial            | Reads existing counts; the data writer does not populate `nan_value_counts`.                                                                      |
| Name mapping                                             | :white_check_mark: | Reads imported files without field IDs using an existing `schema.name-mapping.default`.                                                           |
| Statistics-file generation and query use                 | :x:                | Preserves existing statistics-file metadata; no statistics-file creation or query consumption.                                                    |
| Snapshot/reference history                               | :white_check_mark: | Preserves history and reads existing refs.                                                                                                        |

## Version 2: Delete Files

| Feature                              | Read               | Write              | Notes                                                                                                                            |
| ------------------------------------ | ------------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Sequence numbers and inheritance     | :white_check_mark: | :white_check_mark: | Used to determine delete applicability.                                                                                          |
| Manifest and data-file content types | :white_check_mark: | :white_check_mark: | Distinguishes data, equality deletes, and position deletes.                                                                      |
| Position-delete files                | :white_check_mark: | Partial            | Written by version 2 merge-on-read `MERGE`. Existing files can still be read after an upgrade to version 3.                      |
| Equality-delete files                | :white_check_mark: | Partial            | Reads bind keys by field ID and apply partition/sequence rules; write support is listed under [DML Operations](#dml-operations). |
| Delete-aware scan planning           | :white_check_mark: | —                  | Applies supported deletes before returning rows, including scans with a limit.                                                   |

## Version 3: Extended Types and Capabilities

| Feature                                       | Supported          | Notes                                                                                                                                                             |
| --------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Variant                                       | :white_check_mark: | Reads and writes logical `VARIANT`, including Parquet shredding.                                                                                                  |
| Unknown type                                  | :white_check_mark: | All-null logical fields, including copy-on-write preservation.                                                                                                    |
| `timestamp_ns`, `timestamptz_ns`              | Partial            | Iceberg/Arrow conversion and defaults are implemented; Spark Connect result-schema conversion does not support nanosecond timestamps.                             |
| Geometry and geography                        | :x:                | Binary storage conversion does not provide their logical type semantics.                                                                                          |
| Initial and write defaults                    | Partial            | Applies defaults already present in metadata, including SQL `DEFAULT`. Declaring defaults in `CREATE TABLE` or changing them with `ALTER TABLE` is not supported. |
| Row lineage and first-row-ID inheritance      | :white_check_mark: | Assigns lineage on writes; copy-on-write preserves row IDs and advances update sequence numbers.                                                                  |
| Multi-argument partition/sort transforms      | :x:                | —                                                                                                                                                                 |
| Deletion vectors in Puffin files              | :x:                | Neither read nor write; scans reject these delete artifacts.                                                                                                      |
| Encryption keys and AES-GCM stream encryption | :x:                | —                                                                                                                                                                 |

## Catalogs and Maintenance

Sail supports filesystem-backed tables and catalog-backed tables through [Iceberg REST](../../catalog/iceberg-rest), [AWS Glue](../../catalog/glue), and [Hive Metastore](../../catalog/hms).
For a catalog-backed table, use its catalog name so that reads use the catalog's metadata location and commits publish the new metadata through that catalog.

| Feature                            | Supported          | Notes                                                                                                                  |
| ---------------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Catalog-backed reads and commits   | :white_check_mark: | Uses the catalog's metadata pointer; filesystem discovery and `version-hint.text` cannot replace it.                   |
| Iceberg REST views                 | Partial            | Create, load, list, and drop when the server provides those endpoints; other lifecycle operations are not implemented. |
| Iceberg SQL UDF specification      | :x:                | —                                                                                                                      |
| Snapshot expiration                | :x:                | —                                                                                                                      |
| Data-file compaction               | :x:                | `rewrite_data_files`.                                                                                                  |
| Position-delete rewrite procedures | :x:                | —                                                                                                                      |
| Multi-table atomic transactions    | :x:                | —                                                                                                                      |
| Z-order clustering                 | :x:                | —                                                                                                                      |
| Write-audit-publish                | :x:                | —                                                                                                                      |
| Streaming reads and writes         | :x:                | —                                                                                                                      |
