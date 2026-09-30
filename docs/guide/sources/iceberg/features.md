---
title: Supported Features
rank: 2
---

# Supported Features

The tables below describe which Iceberg features Sail supports.
:white_check_mark: indicates support, :warning: identifies the supported cases in the notes, and :construction: means not supported.

## Format Versions

Sail creates **version 2** Iceberg tables by default. Set the `format-version` table property when creating a table to use another supported version, or change it later to upgrade a table.

| Feature                           | Supported          | Notes                                                                        |
| --------------------------------- | ------------------ | ---------------------------------------------------------------------------- |
| Format versions 1, 2, and 3       | :white_check_mark: | Reads and writes table metadata. Version-specific features are listed below. |
| Format-version upgrades           | :white_check_mark: | Through the `format-version` table property.                                 |
| Format-version downgrades         | :construction:     | Iceberg permits format-version upgrades only.                                |
| Parquet data and delete files     | :white_check_mark: | Delete-file support is listed by version below.                              |
| Puffin deletion-vector files      | :white_check_mark: | Version 3 merge-on-read operations.                                          |
| Avro manifests and manifest lists | :white_check_mark: | —                                                                            |
| Avro and ORC data files           | :construction:     | —                                                                            |

## Core Table Operations

| Feature                                            | Supported          | Notes                                                                                                                                                                                                       |
| -------------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table creation, CTAS, and replacement              | :warning:          | Through SQL and the DataFrame writer APIs; replacement is available in the Memory catalog. See [Table DDL](#table-ddl).                                                                                     |
| Current snapshot reads, append, and full overwrite | :white_check_mark: | Both partitioned and non-partitioned tables.                                                                                                                                                                |
| Metadata-as-data reads                             | :white_check_mark: | Optional manifest scanning during query execution instead of loading the file list on the driver.                                                                                                           |
| Predicate pushdown and file pruning                | :white_check_mark: | Uses partition transforms and available file metrics.                                                                                                                                                       |
| Metadata aggregate optimization                    | :white_check_mark: | Eligible `COUNT`, `MIN`, and `MAX` queries use exact metadata. Incomplete metrics or applicable deletes can require scanning data.                                                                          |
| Schema evolution on write                          | :white_check_mark: | `mergeSchema` adds fields and applies supported promotions. `overwriteSchema` replaces the schema during full overwrite. The two options cannot be combined.                                                |
| Predicate overwrite                                | :warning:          | `DataFrameWriterV2.overwrite(condition)` on identity-partition columns. Requires compatible live partition specs and no active delete files.                                                                |
| Dynamic partition overwrite                        | :warning:          | `overwritePartitions()` or `overwrite-mode = dynamic` on existing tables with compatible live partition specs and no active delete files. Replaces partitions present in the input. Empty input is a no-op. |
| Time travel                                        | :white_check_mark: | Snapshot ID, timestamp, or an existing branch/tag reference. Requires the referenced metadata and data files.                                                                                               |
| Branch/tag creation and branch writes              | :construction:     | —                                                                                                                                                                                                           |
| Table property DDL                                 | :warning:          | `SET/UNSET TBLPROPERTIES` and format upgrades through Memory, HMS, Glue, and Iceberg REST; see [Table DDL](#table-ddl) for server limitations.                                                              |
| Column type and default DDL                        | :warning:          | Top-level type promotions and `SET/DROP DEFAULT`; defaults require format version 3 and typed literals. See [Table DDL](#table-ddl).                                                                        |
| Commit conflict handling                           | :warning:          | Validates metadata requirements and the expected snapshot, with limited metadata publication retries. Row-level conflicts require replanning, including with `snapshot` isolation.                          |

## Table DDL

The [Iceberg DDL matrix](../../catalog/index.md#iceberg) lists each operation across
all catalog providers, including unsupported DDL. It covers creation, registration,
replacement, schema changes, defaults, properties, partition evolution, write
ordering, identifier fields, branches, tags, and dropping tables.

## DML Operations

Copy-on-write rewrites affected data files, while merge-on-read records deletes separately. Sail uses copy-on-write by default for `DELETE`, `UPDATE`, and `MERGE INTO`. The `write.delete.mode`, `write.update.mode`, and `write.merge.mode` table properties let you choose a mode for each operation. For merge-on-read, version 2 `MERGE` uses Parquet position-delete files, while version 3 DML uses Puffin deletion vectors.

| Operation                                       | Copy-on-write (v1–v3) | Merge-on-read (v2) | Merge-on-read (v3) | Notes                                                                                                                                                                                  |
| ----------------------------------------------- | --------------------- | ------------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DELETE`                                        | :white_check_mark:    | :warning:          | :white_check_mark: | V2 uses equality-delete files on unpartitioned tables, with all columns as equality keys. Nested, floating-point, `unknown`, and `variant` fields are unsupported in that writer path. |
| `UPDATE`                                        | :white_check_mark:    | :construction:     | :white_check_mark: | V3 merge-on-read supports updates that move rows between partitions.                                                                                                                   |
| `MERGE INTO` with inserts, updates, and deletes | :white_check_mark:    | :white_check_mark: | :white_check_mark: | Matched, insert, and `WHEN NOT MATCHED BY SOURCE` clauses. Multiple source matches cannot update one target row.                                                                       |
| `MERGE WITH SCHEMA EVOLUTION`                   | :construction:        | :construction:     | :construction:     | —                                                                                                                                                                                      |

When metadata shows that a delete removes every row in a file, Sail removes the file reference directly, including for partitioned tables. Version 3 merge-on-read works with partitioned and non-partitioned tables. Merge-on-read `UPDATE` (version 3) and `MERGE` (versions 2 and 3) append replacement data for updated rows.

## Metadata, Schema, and Layout

Across the supported format versions, Sail reads and writes table metadata, tracks fields by ID, and uses available partition and file metrics when planning queries. The table below lists the details and limits.

| Feature                                                  | Supported          | Notes                                                                                                                                             |
| -------------------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table metadata, snapshots, manifest lists, and manifests | :white_check_mark: | Reads and writes the supported version-specific fields.                                                                                           |
| Field IDs and schema history                             | :white_check_mark: | Resolves evolved columns by ID, including nested fields.                                                                                          |
| Partition transforms                                     | :white_check_mark: | Identity, bucket, truncate, year, month, day, and hour. Existing void transforms are handled.                                                     |
| Partition evolution                                      | :warning:          | Reads existing specs and writes using the current spec. Schema-replacing overwrite can change partitioning. No partition-evolution DDL.           |
| Sort orders                                              | :warning:          | Honors supported existing single-source sort transforms and records sort-order IDs on data files. No sort-order DDL or multi-argument transforms. |
| Column metrics                                           | :white_check_mark: | Uses file counts, null counts, bounds, and other available metrics for planning.                                                                  |
| NaN value counts                                         | :warning:          | Reads existing counts. The data writer does not populate `nan_value_counts`.                                                                      |
| Name mapping                                             | :white_check_mark: | Reads imported files without field IDs using an existing `schema.name-mapping.default`.                                                           |
| Statistics-file generation and query use                 | :construction:     | Preserves existing statistics-file metadata. No statistics-file creation or query consumption.                                                    |
| Snapshot/reference history                               | :white_check_mark: | Preserves history and reads existing refs.                                                                                                        |

## Version 2: Delete Files

| Feature                              | Read               | Write              | Notes                                                                                                                                               |
| ------------------------------------ | ------------------ | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sequence numbers and inheritance     | :white_check_mark: | :white_check_mark: | Used to determine delete applicability.                                                                                                             |
| Manifest and data-file content types | :white_check_mark: | :white_check_mark: | Distinguishes data, equality deletes, and position deletes.                                                                                         |
| Position-delete files                | :white_check_mark: | :warning:          | Written by version 2 merge-on-read `MERGE`. Existing files can still be read after an upgrade to version 3.                                         |
| Equality-delete files                | :white_check_mark: | :warning:          | Written by version 2 merge-on-read `DELETE`. Reads bind keys by field ID and apply partition/sequence rules. See [DML Operations](#dml-operations). |
| Delete-aware scan planning           | :white_check_mark: | :construction:     | Applies supported deletes before returning rows, including scans with a limit.                                                                      |

## Version 3: Extended Types and Capabilities

| Feature                                       | Supported          | Notes                                                                                                                                                                               |
| --------------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Variant                                       | :white_check_mark: | Reads and writes logical `VARIANT`, including Parquet shredding.                                                                                                                    |
| Unknown type                                  | :white_check_mark: | All-null logical fields, including copy-on-write preservation.                                                                                                                      |
| `timestamp_ns`, `timestamptz_ns`              | :warning:          | Iceberg/Arrow conversion and defaults are implemented. Spark Connect result-schema conversion does not support nanosecond timestamps.                                               |
| Geometry and geography                        | :construction:     | Binary storage conversion does not provide their logical type semantics.                                                                                                            |
| Initial and write defaults                    | :warning:          | Reads existing defaults and supports SQL `DEFAULT`, `CREATE TABLE` defaults, and `ALTER COLUMN SET/DROP DEFAULT`. DDL requires typed literals and a server that supports version 3. |
| Row lineage and first-row-ID inheritance      | :white_check_mark: | Assigns row IDs on insert. Copy-on-write and merge-on-read updates preserve row IDs and advance update sequence numbers.                                                            |
| Multi-argument partition/sort transforms      | :construction:     | —                                                                                                                                                                                   |
| Deletion vectors in Puffin files              | :white_check_mark: | Reads and writes vectors, combining prior positional deletes when replacing the vector for a data file.                                                                             |
| Encryption keys and AES-GCM stream encryption | :construction:     | —                                                                                                                                                                                   |

## Catalogs and Maintenance

Sail works with filesystem-backed tables and tables in [Iceberg REST](../../catalog/iceberg-rest), [AWS Glue](../../catalog/glue), or [Hive Metastore](../../catalog/hms). For a catalog-backed table, use its catalog name so reads follow the catalog metadata location and commits publish new metadata through the catalog.

| Feature                            | Supported          | Notes                                                                                                                  |
| ---------------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| Catalog-backed reads and commits   | :white_check_mark: | Uses the catalog metadata pointer. Filesystem discovery and `version-hint.text` cannot replace it.                     |
| Iceberg REST views                 | :warning:          | Create, load, list, and drop when the server provides those endpoints. Other lifecycle operations are not implemented. |
| Iceberg SQL UDF specification      | :construction:     | —                                                                                                                      |
| Snapshot expiration                | :construction:     | —                                                                                                                      |
| Data-file compaction               | :construction:     | `rewrite_data_files`.                                                                                                  |
| Position-delete rewrite procedures | :construction:     | —                                                                                                                      |
| Multi-table atomic transactions    | :construction:     | —                                                                                                                      |
| Z-order clustering                 | :construction:     | —                                                                                                                      |
| Write-audit-publish                | :construction:     | —                                                                                                                      |
| Streaming reads and writes         | :construction:     | —                                                                                                                      |
