---
title: Supported Features
rank: 2
---

# Supported Features

This page lists Delta Lake features supported in Sail.
:white_check_mark: means supported, **Partial** means supported for the cases listed, and :x: means not supported.

## Protocol Compatibility

The Delta protocol defines reader and writer requirements separately. Legacy protocols identify feature requirements through the protocol version and table metadata.

| Feature                                  | Supported          | Notes                                                                 |
| ---------------------------------------- | ------------------ | --------------------------------------------------------------------- |
| Reader protocol versions 1–3             | :white_check_mark: | All required `readerFeatures` must be supported.                      |
| Writer protocol versions 1–7             | :white_check_mark: | All required `readerFeatures` and `writerFeatures` must be supported. |
| Unknown or unsupported required features | :x:                | Sail rejects the operation requiring the feature.                     |

## Core Table Operations

| Feature                                       | Supported          | Notes                                                                                                                     |
| --------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| Table creation, CTAS, and replacement         | :white_check_mark: | Through SQL and the DataFrame writer APIs.                                                                                |
| Snapshot reads, append, and full overwrite    | :white_check_mark: | Both partitioned and non-partitioned tables.                                                                              |
| Conditional overwrite                         | :white_check_mark: | `replaceWhere` and SQL `REPLACE WHERE`.                                                                                   |
| Dynamic partition overwrite                   | :x:                | `DataFrameWriterV2.overwritePartitions()` is not supported for Delta.                                                     |
| Partition pruning and file-statistics pruning | :white_check_mark: | Uses partition values and statistics recorded in the log.                                                                 |
| Metadata aggregate optimization               | :white_check_mark: | Eligible aggregates can use exact file statistics; other queries scan data.                                               |
| Schema validation and evolution               | :white_check_mark: | `mergeSchema`, `overwriteSchema` with full overwrite, and `MERGE WITH SCHEMA EVOLUTION`.                                  |
| Time travel                                   | :white_check_mark: | Version or timestamp through read options or SQL; requires retained log/checkpoint state and data files for that version. |
| Table property DDL                            | Partial            | `SET/UNSET TBLPROPERTIES`; see [Catalog Integration](#catalog-integration) for catalog-managed tables.                    |
| Column type and default DDL                   | Partial            | Supported type widening and `SET/DROP DEFAULT`; not all `ALTER TABLE` forms are implemented.                              |
| Optimistic commit conflict handling           | Partial            | Creation and blind appends can retry compatible conflicts. Other writes fail on a competing commit and must be replanned. |

## DML Operations

| Operation                                       | Copy-on-write      | Deletion vectors   | Notes                                                                                                            |
| ----------------------------------------------- | ------------------ | ------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `DELETE`                                        | :white_check_mark: | :white_check_mark: | —                                                                                                                |
| `UPDATE`                                        | :white_check_mark: | :white_check_mark: | —                                                                                                                |
| `MERGE INTO` with inserts, updates, and deletes | :white_check_mark: | :white_check_mark: | Matched, insert, and `WHEN NOT MATCHED BY SOURCE` clauses. Multiple source matches cannot update one target row. |

Delta Lake represents row changes through file rewrites (copy-on-write) or deletion vectors.
With deletion vectors, updates mark old rows as deleted and append replacement rows.

## Table Features

### Reader-Writer Features

| Protocol feature                                                   | Read               | Write              | Notes                                                                                                                            |
| ------------------------------------------------------------------ | ------------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Column mapping (`columnMapping`)                                   | :white_check_mark: | :white_check_mark: | `name` and `id` modes; logical names are distinct from Parquet field names and IDs.                                              |
| Deletion vectors (`deletionVectors`)                               | :white_check_mark: | :white_check_mark: | Applies existing vectors on reads and writes them for supported row-level operations.                                            |
| Timestamp without timezone (`timestampNtz`)                        | :white_check_mark: | :white_check_mark: | `TIMESTAMP_NTZ`.                                                                                                                 |
| Type widening (`typeWidening`, `typeWidening-preview`)             | :white_check_mark: | :white_check_mark: | New widening requires `delta.enableTypeWidening`; only supported type changes are accepted.                                      |
| Variant (`variantType`, `variantType-preview`)                     | :white_check_mark: | :white_check_mark: | Logical `VARIANT` values stored in Parquet.                                                                                      |
| Variant shredding (`variantShredding`, `variantShredding-preview`) | :white_check_mark: | :white_check_mark: | Shredded reads and writes; writes require shredding enablement.                                                                  |
| V2 checkpoints (`v2Checkpoint`)                                    | :white_check_mark: | :white_check_mark: | Checkpoint policy controls the checkpoint format.                                                                                |
| VACUUM protocol check (`vacuumProtocolCheck`)                      | :white_check_mark: | :white_check_mark: | Accepted for ordinary reads and writes; command support is listed under [Maintenance and Streaming](#maintenance-and-streaming). |
| Catalog-managed tables (`catalogManaged`)                          | Partial            | Partial            | Requires Unity Catalog commit and replay support; see [Catalog Integration](#catalog-integration).                               |

### Writer Features and Constraints

The following checklist covers feature-specific write behavior.

| Feature                                                      | Write support      | Notes                                                                                                                                          |
| ------------------------------------------------------------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Append-only tables (`appendOnly`)                            | :white_check_mark: | Data-changing removals are rejected when append-only is enabled.                                                                               |
| `NOT NULL` constraints                                       | :white_check_mark: | Enforced during writes.                                                                                                                        |
| Legacy column invariant expressions (`delta.invariants`)     | :x:                | The `invariants` protocol flag is accepted; legacy expressions are not enforced.                                                               |
| `CHECK` constraints (`checkConstraints`)                     | :white_check_mark: | Writes must satisfy constraints. `ADD CONSTRAINT` validates existing rows; both false and null results violate a constraint.                   |
| Generated columns (`generatedColumns`)                       | :white_check_mark: | Computes omitted values and validates explicitly supplied values.                                                                              |
| Default columns (`allowColumnDefaults`)                      | :white_check_mark: | Omitted columns and explicit `DEFAULT`; `ALTER COLUMN SET/DROP DEFAULT`.                                                                       |
| Identity columns (`identityColumns`)                         | :white_check_mark: | Non-partition `BIGINT` columns with nonzero positive or negative steps. `GENERATED ALWAYS` rejects explicit values; `BY DEFAULT` accepts them. |
| In-commit timestamps (`inCommitTimestamp`)                   | :white_check_mark: | Writes commit timestamps and uses enablement metadata for time travel.                                                                         |
| Change Data Feed (`changeDataFeed`)                          | :x:                | No CDF query or change-file writer.                                                                                                            |
| Row tracking (`rowTracking`)                                 | :x:                | No row-tracking write support.                                                                                                                 |
| Domain metadata (`domainMetadata`)                           | :x:                | Replaying existing actions does not enable writes requiring this feature.                                                                      |
| Iceberg compatibility (`icebergCompatV1`, `icebergCompatV2`) | :x:                | No UniForm/Iceberg-compatible writer.                                                                                                          |
| Clustered tables (`clustering`)                              | :x:                | No clustered-table writer.                                                                                                                     |

## Log and Checkpoint Formats

| Format or action                                                    | Read/replay        | Write              | Notes                                                                                    |
| ------------------------------------------------------------------- | ------------------ | ------------------ | ---------------------------------------------------------------------------------------- |
| Parquet data files                                                  | :white_check_mark: | :white_check_mark: | Partition values come from the transaction log.                                          |
| JSON commits: `protocol`, `metaData`, `add`, `remove`, `commitInfo` | :white_check_mark: | :white_check_mark: | Snapshot reconstruction and table commits.                                               |
| `txn` actions                                                       | :white_check_mark: | Partial            | Preserves transaction state in checkpoints.                                              |
| `domainMetadata` actions                                            | :white_check_mark: | Partial            | Replays and preserves existing state in checkpoints; no public domain-update operation.  |
| Change data files (`cdc`)                                           | :x:                | :x:                | Ordinary snapshot reads do not read change data files.                                   |
| Classic Parquet checkpoints                                         | :white_check_mark: | :white_check_mark: | Includes supported parsed statistics and partition fields.                               |
| UUID-named V2 checkpoints and Parquet sidecars                      | :white_check_mark: | :white_check_mark: | Reads JSON or Parquet top-level checkpoints; writes use `delta.checkpointPolicy = 'v2'`. |
| Multi-part checkpoints                                              | :white_check_mark: | :x:                | Reads complete sets; incomplete sets are ignored.                                        |
| Log compaction files                                                | :white_check_mark: | :x:                | Uses compacted ranges during replay; no compaction-file producer.                        |
| `_last_checkpoint`                                                  | :white_check_mark: | :white_check_mark: | Checkpoint discovery hint.                                                               |
| Version checksum files                                              | :white_check_mark: | :white_check_mark: | Checksum-based snapshot information and validation.                                      |

## Catalog Integration

Sail integrates with [Unity Catalog](../../catalog/unity), [AWS Glue](../../catalog/glue), and [Hive Metastore](../../catalog/hms) for Delta tables.

| Feature                                          | Supported          | Notes                                                                                                                        |
| ------------------------------------------------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| Catalog registration of filesystem-backed tables | :white_check_mark: | Supported through Unity Catalog, AWS Glue, and Hive Metastore.                                                               |
| `catalogManaged` reads and writes by table name  | :white_check_mark: | Uses Unity Catalog to obtain ratified commits and publish writes, including commits not yet published as ordinary log files. |
| `catalogManaged` direct path access              | :x:                | Requires the replay context supplied by the catalog.                                                                         |
| `catalogManaged` metadata `ALTER TABLE`          | :x:                | Includes table property changes.                                                                                             |

## Maintenance and Streaming

| Operation                   | Supported |
| --------------------------- | --------- |
| `VACUUM`                    | :x:       |
| `OPTIMIZE`                  | :x:       |
| `RESTORE`                   | :x:       |
| Structured Streaming reads  | :x:       |
| Structured Streaming writes | :x:       |
