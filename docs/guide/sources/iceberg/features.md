---
title: Supported Features
rank: 2
---

# Supported Features

The tables below describe which Iceberg features Sail supports. A :white_check_mark: indicates support, :white_check_mark: (Partial) identifies the supported cases in the notes, and :x: indicates that the feature is unavailable.

## Format Versions

Sail creates **version 2** Iceberg tables by default. Set the `format-version` table property when creating a table to use another supported version, or change it later to upgrade a table.

| Feature                           | Supported          | Notes                                                                        |
| --------------------------------- | ------------------ | ---------------------------------------------------------------------------- |
| Format versions 1, 2, and 3       | :white_check_mark: | Reads and writes table metadata. Version-specific features are listed below. |
| Format-version upgrades           | :white_check_mark: | Through the `format-version` table property.                                 |
| Format-version downgrades         | :x:                | —                                                                            |
| Parquet data and delete files     | :white_check_mark: | Delete-file support is listed by version below.                              |
| Puffin deletion-vector files      | :white_check_mark: | Version 3 merge-on-read operations.                                          |
| Avro manifests and manifest lists | :white_check_mark: | —                                                                            |
| Avro and ORC data files           | :x:                | —                                                                            |

## Core Table Operations

| Feature                                            | Supported                    | Notes                                                                                                                                                                                                       |
| -------------------------------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table creation, CTAS, and replacement              | :white_check_mark:           | Through SQL and the DataFrame writer APIs.                                                                                                                                                                  |
| Current snapshot reads, append, and full overwrite | :white_check_mark:           | Both partitioned and non-partitioned tables.                                                                                                                                                                |
| Metadata-as-data reads                             | :white_check_mark:           | Optional manifest scanning during query execution instead of loading the file list on the driver.                                                                                                           |
| Predicate pushdown and file pruning                | :white_check_mark:           | Uses partition transforms and available file metrics.                                                                                                                                                       |
| Metadata aggregate optimization                    | :white_check_mark:           | Eligible `COUNT`, `MIN`, and `MAX` queries use exact metadata. Incomplete metrics or applicable deletes can require scanning data.                                                                          |
| Schema evolution on write                          | :white_check_mark:           | `mergeSchema` adds fields and applies supported promotions. `overwriteSchema` replaces the schema during full overwrite. The two options cannot be combined.                                                |
| Predicate overwrite                                | :white_check_mark: (Partial) | `DataFrameWriterV2.overwrite(condition)` on identity-partition columns. Requires compatible live partition specs and no active delete files.                                                                |
| Dynamic partition overwrite                        | :white_check_mark: (Partial) | `overwritePartitions()` or `overwrite-mode = dynamic` on existing tables with compatible live partition specs and no active delete files. Replaces partitions present in the input. Empty input is a no-op. |
| Time travel                                        | :white_check_mark:           | Snapshot ID, timestamp, or an existing branch/tag reference. Requires the referenced metadata and data files.                                                                                               |
| Branch/tag creation and branch writes              | :x:                          | —                                                                                                                                                                                                           |
| Table property DDL                                 | :white_check_mark: (Partial) | Filesystem-backed tables support `SET/UNSET TBLPROPERTIES`, including format upgrades. Catalog-managed metadata `ALTER TABLE` is not supported.                                                             |
| Commit conflict handling                           | :white_check_mark: (Partial) | Validates metadata requirements and the expected snapshot, with limited metadata publication retries. Row-level conflicts require replanning, including with `snapshot` isolation.                          |

## DML Operations

Copy-on-write rewrites affected data files, while merge-on-read records deletes separately. Sail uses copy-on-write by default for `DELETE`, `UPDATE`, and `MERGE INTO`. The `write.delete.mode`, `write.update.mode`, and `write.merge.mode` table properties let you choose a mode for each operation. For merge-on-read, version 2 `MERGE` uses Parquet position-delete files, while version 3 DML uses Puffin deletion vectors.

| Operation                                       | Copy-on-write (v1–v3) | Merge-on-read (v2)           | Merge-on-read (v3) | Notes                                                                                                                                                                                  |
| ----------------------------------------------- | --------------------- | ---------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DELETE`                                        | :white_check_mark:    | :white_check_mark: (Partial) | :white_check_mark: | V2 uses equality-delete files on unpartitioned tables, with all columns as equality keys. Nested, floating-point, `unknown`, and `variant` fields are unsupported in that writer path. |
| `UPDATE`                                        | :white_check_mark:    | :x:                          | :white_check_mark: | V3 merge-on-read supports updates that move rows between partitions.                                                                                                                   |
| `MERGE INTO` with inserts, updates, and deletes | :white_check_mark:    | :white_check_mark:           | :white_check_mark: | Matched, insert, and `WHEN NOT MATCHED BY SOURCE` clauses. Multiple source matches cannot update one target row.                                                                       |
| `MERGE WITH SCHEMA EVOLUTION`                   | :x:                   | :x:                          | :x:                | —                                                                                                                                                                                      |

When metadata shows that a delete removes every row in a file, Sail removes the file reference directly, including for partitioned tables. Version 3 merge-on-read works with partitioned and non-partitioned tables. Its `UPDATE` and `MERGE` operations append replacement data for updated rows.

## Metadata, Schema, and Layout

Across the supported format versions, Sail reads and writes table metadata, tracks fields by ID, and uses available partition and file metrics when planning queries. The table below lists the details and limits.

| Feature                                                  | Supported                    | Notes                                                                                                                                             |
| -------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table metadata, snapshots, manifest lists, and manifests | :white_check_mark:           | Reads and writes the supported version-specific fields.                                                                                           |
| Field IDs and schema history                             | :white_check_mark:           | Resolves evolved columns by ID, including nested fields.                                                                                          |
| Partition transforms                                     | :white_check_mark:           | Identity, bucket, truncate, year, month, day, and hour. Existing void transforms are handled.                                                     |
| Partition evolution                                      | :white_check_mark: (Partial) | Reads existing specs and writes using the current spec. Schema-replacing overwrite can change partitioning. No partition-evolution DDL.           |
| Sort orders                                              | :white_check_mark: (Partial) | Honors supported existing single-source sort transforms and records sort-order IDs on data files. No sort-order DDL or multi-argument transforms. |
| Column metrics                                           | :white_check_mark:           | Uses file counts, null counts, bounds, and other available metrics for planning.                                                                  |
| NaN value counts                                         | :white_check_mark: (Partial) | Reads existing counts. The data writer does not populate `nan_value_counts`.                                                                      |
| Name mapping                                             | :white_check_mark:           | Reads imported files without field IDs using an existing `schema.name-mapping.default`.                                                           |
| Statistics-file generation and query use                 | :x:                          | Preserves existing statistics-file metadata. No statistics-file creation or query consumption.                                                    |
| Snapshot/reference history                               | :white_check_mark:           | Preserves history and reads existing refs.                                                                                                        |

## Version 2: Delete Files

| Feature                              | Read               | Write                        | Notes                                                                                                                                               |
| ------------------------------------ | ------------------ | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sequence numbers and inheritance     | :white_check_mark: | :white_check_mark:           | Used to determine delete applicability.                                                                                                             |
| Manifest and data-file content types | :white_check_mark: | :white_check_mark:           | Distinguishes data, equality deletes, and position deletes.                                                                                         |
| Position-delete files                | :white_check_mark: | :white_check_mark: (Partial) | Written by version 2 merge-on-read `MERGE`. Existing files can still be read after an upgrade to version 3.                                         |
| Equality-delete files                | :white_check_mark: | :white_check_mark: (Partial) | Written by version 2 merge-on-read `DELETE`. Reads bind keys by field ID and apply partition/sequence rules. See [DML Operations](#dml-operations). |
| Delete-aware scan planning           | :white_check_mark: | —                            | Applies supported deletes before returning rows, including scans with a limit.                                                                      |

## Version 3: Extended Types and Capabilities

| Feature                                       | Supported                    | Notes                                                                                                                                                             |
| --------------------------------------------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Variant                                       | :white_check_mark:           | Reads and writes logical `VARIANT`, including Parquet shredding.                                                                                                  |
| Unknown type                                  | :white_check_mark:           | All-null logical fields, including copy-on-write preservation.                                                                                                    |
| `timestamp_ns`, `timestamptz_ns`              | :white_check_mark: (Partial) | Iceberg/Arrow conversion and defaults are implemented. Spark Connect result-schema conversion does not support nanosecond timestamps.                             |
| Geometry and geography                        | :x:                          | Binary storage conversion does not provide their logical type semantics.                                                                                          |
| Initial and write defaults                    | :white_check_mark: (Partial) | Applies defaults already present in metadata, including SQL `DEFAULT`. Declaring defaults in `CREATE TABLE` or changing them with `ALTER TABLE` is not supported. |
| Row lineage and first-row-ID inheritance      | :white_check_mark:           | Assigns row IDs on insert. Copy-on-write and merge-on-read updates preserve row IDs and advance update sequence numbers.                                          |
| Multi-argument partition/sort transforms      | :x:                          | —                                                                                                                                                                 |
| Deletion vectors in Puffin files              | :white_check_mark:           | Reads and writes vectors, combining prior positional deletes when replacing the vector for a data file.                                                           |
| Encryption keys and AES-GCM stream encryption | :x:                          | —                                                                                                                                                                 |

## Catalogs and Maintenance

Sail works with filesystem-backed tables and tables in [Iceberg REST](../../catalog/iceberg-rest), [AWS Glue](../../catalog/glue), or [Hive Metastore](../../catalog/hms). For a catalog-backed table, use its catalog name so reads follow the catalog metadata location and commits publish new metadata through the catalog.

| Feature                            | Supported                    | Notes                                                                                                                  |
| ---------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Catalog-backed reads and commits   | :white_check_mark:           | Uses the catalog metadata pointer. Filesystem discovery and `version-hint.text` cannot replace it.                     |
| Iceberg REST views                 | :white_check_mark: (Partial) | Create, load, list, and drop when the server provides those endpoints. Other lifecycle operations are not implemented. |
| Iceberg SQL UDF specification      | :x:                          | —                                                                                                                      |
| Snapshot expiration                | :x:                          | —                                                                                                                      |
| Data-file compaction               | :x:                          | `rewrite_data_files`.                                                                                                  |
| Position-delete rewrite procedures | :x:                          | —                                                                                                                      |
| Multi-table atomic transactions    | :x:                          | —                                                                                                                      |
| Z-order clustering                 | :x:                          | —                                                                                                                      |
| Write-audit-publish                | :x:                          | —                                                                                                                      |
| Streaming reads and writes         | :x:                          | —                                                                                                                      |
