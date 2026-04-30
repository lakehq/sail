---
title: Hive Metastore
---

# Hive Metastore Development Notes

This page documents the current HMS implementation and test strategy in Sail for contributors and maintainers.

## Maintainer Summary

The current HMS work establishes a metadata-focused compatibility layer for Apache Hive Metastore.

What maintainers can rely on today:

- Plain HMS catalog connectivity over Thrift.
- Kerberos-secured HMS connectivity over Thrift SASL.
- Kerberos SASL frame wrapping (`auth-int` / `auth-conf`) with configurable minimum QOP (`min_sasl_qop`).
- HMS endpoint lists with automatic failover on retryable transport/Thrift errors.
- Basic metadata CRUD for databases and tables through `HmsCatalogProvider`.
- A self-contained Kerberos integration harness built with `testcontainers`.
- CI wiring for the ignored catalog-test lane.

What maintainers should not infer from the current test suite:

- Broad production readiness across HMS distributions and versions.
- Support for Hive ACID or transactional HMS APIs.
- Support for delegation tokens or TLS.

## What Green Means

The Kerberos HMS ignored integration test target is:

```bash
cargo test -p sail-catalog-hms --test kerberos_integration_tests -- --ignored
```

When this target is green, it means Sail successfully:

- boots a local MIT KDC and Kerberized Hive Metastore container
- obtains client credentials with `kinit`
- authenticates to HMS over Thrift SASL using Kerberos
- performs database create/get/drop operations
- performs table create/get/drop operations
- fails cleanly when Kerberos credentials are missing

This is strong evidence that the Kerberos transport and basic HMS metadata path work end to end.

It is not evidence that Sail supports:

- Hive ACID transaction APIs
- non-Apache HMS distributions
- production RDBMS-backed HMS deployments

It is only partial evidence for HA behavior:

- the current test suite covers ordered endpoint failover and retryable transport errors
- it does not yet prove broader operational behavior such as DNS churn under long-lived production traffic, vendor-specific HA frontends, or multi-region deployment patterns

## Local Verification

### Fast checks

```bash
cargo test -p sail-catalog-hms --lib
```

This covers HMS provider validation logic, auth config parsing, Kerberos session behavior, and Thrift SASL handshake unit tests.

### Full Kerberos harness

```bash
cargo test -p sail-catalog-hms --test kerberos_integration_tests -- --ignored
```

The preferred local environment is the repo devcontainer because it includes:

- Kerberos client tools
- the GSSAPI runtime used by the dynamic loader
- Docker access for `testcontainers`

Host-native macOS runs may work, but Linux in the devcontainer or CI is the supported path for the Kerberos harness.

## CI Behavior

The Kerberos HMS harness runs in the ignored catalog-test lane.

On pull requests, that lane is enabled when either:

- the PR has the `run catalog tests` label
- the head commit message contains `[catalog test]` or `[catalog tests]`

On pushes to `main`, the catalog-test lane runs automatically.

Relevant workflow locations:

- `.github/workflows/build.yml`
- `.github/workflows/rust-tests.yml`

The Rust test workflow installs Kerberos runtime/client packages before running ignored tests so the host-side `kinit` step and Sail's dynamic GSSAPI loading both work in CI.

## Harness Layout

The HMS Kerberos harness lives in:

- `crates/sail-catalog-hms/tests/common/mod.rs`
- `crates/sail-catalog-hms/tests/kerberos_integration_tests.rs`
- `crates/sail-catalog-hms/tests/fixtures/kerberos-kdc/`

The harness currently:

- builds a local MIT KDC image from repo fixtures
- provisions an HMS service principal and a client principal
- starts a Kerberized Hive Metastore container
- writes active `hive-site.xml` and `core-site.xml` into the container's default config locations
- uses writable `/tmp`-backed Derby paths for the metastore database during tests

## Current Compatibility Contract

The current HMS provider is intentionally conservative.

Supported contract:

- HMS endpoint list via `uris` (`host:port` or `thrift://host:port`, including comma-flattened entries)
- endpoint-ordered failover with a per-endpoint connect timeout that defaults to `5s` and can be overridden with `connect_timeout_secs`
- DNS is re-resolved when Sail builds a new connection for an endpoint instead of pinning the startup-resolved address forever
- Kerberos principal configured with `auth = "kerberos"` and `kerberos_service_principal`
- minimum Kerberos SASL QOP via `min_sasl_qop` (`auth`, `auth_int`, `auth_conf`)
- `_HOST` expansion for the principal based on the currently selected endpoint
- metadata CRUD for databases, tables, and views
- retried create/drop mutations normalize `AlreadyExists` and `NotFound` responses when the earlier attempt likely succeeded and only the response was lost

Security guarantees in this contract:

- Downgrade fail-fast: if the configured `min_sasl_qop` cannot be met by the server SASL layer advertisement, transport creation fails before the HMS client is usable.
- Session-wide enforcement: when `auth_int` or `auth_conf` is negotiated, every subsequent Thrift frame on that connection is wrapped on write and unwrapped on read.

Out of scope for the current implementation:

- transactional HMS methods such as `open_txns`, `lock`, `heartbeat`, `allocate_table_write_ids`, or compaction APIs
- Hortonworks or other distribution-specific compatibility promises
- automatic keytab management inside Sail

## Decision Log

Record HMS interoperability decisions here when they affect test shape,
metadata semantics, or future compatibility work. Keep entries short: context,
decision, consequence, and follow-up if needed.

### 2026-04-28: Spark `spark.sql.*` metadata is internal

Context: Spark data-source tables store provider, schema, partition, bucket,
sort, and statistics metadata in HMS table parameters under `spark.sql.*`.

Decision: Sail consumes supported `spark.sql.*` keys to reconstruct internal
table status, but those keys must not leak through user-visible table
properties.

Consequence: Tests should validate metadata survival through parsed fields
such as format, schema, partitioning, bucketing, sorting, and statistics rather
than asserting that raw `spark.sql.*` keys are visible.

### 2026-04-28: Hide Hive-generated DDL timestamps from user properties

Context: In
`/Users/santosh/IdeaProjects/spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala`,
Spark's `restoreDataSourceTable` strips Hive-generated `DDL_TIME`
(`transient_lastDdlTime`) from restored table properties before surfacing them
to users.

Decision: Sail filters `transient_lastDdlTime` from HMS table properties
alongside internal `spark.sql.*` metadata when building table/view status.

Consequence: User-visible Sail properties stay aligned with Spark's restored
catalog view and avoid leaking metastore bookkeeping timestamps.

### 2026-04-28: Explicit table locations imply external Spark data-source tables

Context: In
`/Users/santosh/IdeaProjects/spark/sql/core/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveSessionCatalog.scala`,
Spark resolves `CREATE TABLE ... USING ... LOCATION ...` as
`CatalogTableType.EXTERNAL`. In
`/Users/santosh/IdeaProjects/spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala`,
Spark's Hive-compatible data-source write path keeps the location both in HMS
storage descriptor location and storage properties `path`.

Decision: Sail treats `CreateTableOptions.location` as an explicit external
location for HMS data-source tables, writing both `sd.location` and SerDe
parameter `path`, and marks the HMS table as `EXTERNAL_TABLE` with raw
`EXTERNAL=TRUE`.

Consequence: Sail's HMS write path matches Spark's external table semantics
when users provide a table location, while location-less creates continue to
use managed-table behavior. Sail filters raw `EXTERNAL` from user-visible table
properties even though HMS needs it internally.

### 2026-04-28: Spark data-source table reads prefer `path` over `sd.location`

Context: In
`/Users/santosh/IdeaProjects/spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala`,
`restoreDataSourceTable` reconstructs the exposed table location from storage
properties `path` and intentionally does not trust HMS `sd.location` as the
public location source for Spark data-source tables.

Decision: When Sail restores an HMS table that carries Spark data-source
metadata, it prefers SerDe/storage property `path` over `sd.location`, falling
back to `sd.location` only when `path` is absent.

Consequence: Sail matches Spark's restored location semantics for cases where
HMS `sd.location` is stale, placeholder, or otherwise diverges from Spark's
authoritative `path` metadata.

### 2026-04-28: Spark qualifies non-URI table locations before HMS writes

Context: In
`/Users/santosh/IdeaProjects/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/SessionCatalog.scala`,
Spark qualifies table locations before handing them to the external catalog:
absolute POSIX paths become file URIs, and relative paths are resolved against
the database location.

Decision: Sail qualifies `CreateTableOptions.location` the same way before
building HMS table metadata.

Consequence: HMS entries written by Sail match Spark's path normalization for
absolute local paths and database-relative locations instead of persisting raw
user input strings.

### 2026-04-28: HMS database LOCATION follows Spark warehouse qualification

Context: In
`/Users/santosh/IdeaProjects/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/command/ddl.scala`,
Spark's `CREATE DATABASE ... LOCATION ...` first turns the SQL string into a
catalog location and then `SessionCatalog.createDatabase` qualifies it against
the warehouse path. In Sail's SQL flow, relative database locations reached the
HMS provider as `file:./...`, which Hive rejected as an invalid URI.

Decision: Normalize `file:./...` back to a relative path at the HMS provider
boundary and qualify relative database locations against the default database
location before writing HMS metadata.

Consequence: Sail SQL `CREATE DATABASE ... LOCATION 'relative/path'` now
matches Spark's effective warehouse-relative behavior and succeeds against HMS.

### 2026-04-28: Spark/HMS Python interop starts with local JVM Spark

Context: The first Python HMS interop milestone needs a reference writer that
can create real Spark HMS metadata without adding a second Spark Connect server
to the harness.

Decision: Use a local classic JVM Spark session with Hive support as the
reference Spark writer. Keep the Sail side on Spark Connect.

Consequence: The fixture temporarily forces `SPARK_API_MODE=classic` and clears
Spark Connect environment variables while building the reference session.
Reference Spark Connect server coverage is deferred.

### 2026-04-28: HMS smoke readiness is query-based

Context: The HMS Thrift socket and Sail Spark Connect server can both accept
connections before the catalog is fully queryable.

Decision: Treat the HMS/Sail harness as ready only after Sail can run
`SHOW DATABASES` against the HMS-backed catalog.

Consequence: The smoke fixture polls the actual catalog query instead of relying
only on TCP port readiness. This avoids transient transport EOF failures during
startup.

### 2026-04-28: Host Spark and HMS container require a shared warehouse path

Context: The Apache Hive test container defaults the warehouse to
`/opt/hive/data/warehouse`, which is meaningful inside the container but not
writable by the host JVM Spark process.

Decision: The Python HMS harness creates a host temp warehouse directory,
mounts it into the HMS container at the same absolute path, and configures HMS
and reference Spark to use that shared location.

Consequence: Managed-table writes by host Spark can create data files in the
same location recorded in HMS and later read by Sail.

### 2026-04-28: Python HMS harness keeps expensive fixtures session-scoped

Context: The HMS Python interop tests pay most of their startup cost when
starting the Hive Metastore container, the Sail Spark Connect server, and the
reference JVM Spark session.

Decision: Keep `hms_warehouse_dir`, `hms_container`, `hms_endpoint`,
`hms_remote`, `hms_spark`, and `reference_spark` session-scoped.

Consequence: HMS tests amortize startup while still exercising the same shared
catalog/server topology used by the roundtrip scenarios.

### 2026-04-28: Create the remote Spark session before the classic JVM session

Context: PySpark 4.1 does not allow starting a remote Spark Connect session
after a classic JVM `SparkSession` already exists in-process. Expanding the HMS
roundtrip suite to run both direction-specific files without the smoke test
surfaced this order dependency.

Decision: Make the session-scoped `reference_spark` fixture depend on
`hms_spark` so the remote Sail-backed session is always created first.

Consequence: The HMS Python harness is stable regardless of pytest collection
order for tests that need both sessions.

### 2026-04-28: Python HMS tests isolate with one database per test

Context: Session-scoped HMS fixtures make startup affordable, but tests must
not share table names or cleanup state.

Decision: Add a function-scoped `hms_database` fixture that derives a safe
database name from the pytest node id, creates it under the shared warehouse
URI, and drops it with `CASCADE` during teardown.

Consequence: Roundtrip tests can use simple table names inside their isolated
database without paying for a new HMS container or reference Spark session per
test.

### 2026-04-28: Roundtrip tests use an explicit test database location

Context: The Hive container pre-creates the `default` database with a
container-local location. Some HMS deployments also do not allow altering core
database location fields after creation.

Decision: Spark-to-Sail roundtrip tests create a dedicated test database with
an explicit shared warehouse location instead of relying on `default`.

Consequence: Roundtrip failures now represent table/file metadata
interoperability rather than inherited container warehouse topology.

### 2026-04-28: Normalize Spark-style local file URIs from HMS

Context: Spark records local HMS table locations as `file:/absolute/path`.
DataFusion's URL path handling needs the canonical hierarchical form
`file:///absolute/path` to read that directory correctly.

Decision: HMS table conversion normalizes `file:/...` locations to
`file:///...` on the read path before Sail builds listing-table scans.

Consequence: Spark-created managed Parquet tables can be read by Sail through
the HMS catalog, while non-file schemes are preserved unchanged.

### 2026-04-28: Sail-to-Spark direct files work before HMS table scans

Context: The Sail-to-Spark roundtrip currently writes a Sail-created Parquet
table through HMS, verifies Sail can read two rows, then asks reference Spark to
read the same table.

Observation: Reference Spark can read the Sail-written Parquet directory
directly and sees the expected rows, but `SELECT * FROM <hms_table>` returns
zero rows through HMS. This isolates the failure to Spark's interpretation of
the HMS table metadata, not the Parquet files.

Observed metadata discrepancies:

- Sail initially wrote an explicit HMS storage descriptor location plus
  `EXTERNAL=TRUE`; reference Spark restored the table as `EXTERNAL` and omitted
  `Location`.
- Removing `EXTERNAL=TRUE` restored the table as `MANAGED`, but reference Spark
  still had `storage.locationUri = None` and scanned zero rows.
- `REFRESH TABLE` did not change the result, so this was not negative lookup or
  file-index staleness.
- The local Spark source in
  `/Users/santosh/IdeaProjects/spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala`
  shows that data-source table restore reads the location from storage
  properties key `path`; `HiveClientImpl.scala` maps storage properties to HMS
  `StorageDescriptor.serdeInfo.parameters`.
- The local Spark source in
  `/Users/santosh/IdeaProjects/spark/sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveClientImpl.scala`
  also documents that `EXTERNAL_TABLE` must carry raw table property
  `EXTERNAL=TRUE`; otherwise Hive metastore can change the table back to
  `MANAGED_TABLE`.
- Row-read roundtrips alone were insufficient: explicit Sail `LOCATION` tables
  initially read successfully in Spark while Spark restored their catalog type
  as `MANAGED`. The interop tests now assert restored type, provider, and
  location in addition to row content.

Decision: For Sail-created managed HMS data-source tables, write the
Spark-compatible location into SerDe parameters as `path` and leave HMS storage
descriptor location unset. For explicit `LOCATION`, write both `sd.location`
and SerDe `path`, set HMS table type to `EXTERNAL_TABLE`, and include raw
`EXTERNAL=TRUE`. On read, restore Sail's internal table location from either
HMS `sd.location` or SerDe `path`, and preserve HMS table type for Spark-style
describe/list output.

Consequence: Reference Spark reconstructs `storage.locationUri` and reads the
Sail-written files through HMS. Sail still filters raw `spark.sql.*` metadata
and raw `EXTERNAL` metadata from user-visible table properties.

Decision: Alter-table interop should validate Spark's data-source table
metadata rewrite path by changing non-location table properties and then
re-reading the table from the other engine.

Rationale: Spark's `HiveExternalCatalog.alterTable` re-adds the internal
storage property `path`, preserves the old HMS storage location when the
logical data location has not changed, and carries old `spark.sql.sources.*`
metadata forward. The externally visible contract for Sail is that provider,
location, table type, and row readability survive non-location alter-table
operations.

Decision: `ALTER TABLE ... SET LOCATION` must update both the HMS storage
descriptor location and Spark's data-source `path` storage property.

Rationale: Spark's HMS restore path treats `path` as the logical scan location
for data-source tables. Updating only the HMS storage descriptor can leave Spark
or Sail reconstructing the old path. The interop tests now verify Spark-to-Sail
and Sail-to-Spark alter-location roundtrips by inserting an old row, changing
location, inserting a new row, and requiring the other engine to see only the
new row at the restored location.

Decision: Sail writes to partitioned generic HMS tables should perform a
post-write partition recovery step for identity partition directories.

Rationale: Spark data-source tables expose dynamic partitions through HMS after
write. Sail's generic Parquet writer materializes Spark-compatible partition
directories, but HMS clients also need partition objects with raw logical values
and partition locations. The post-write recovery scans partition directories
after the data write completes and registers missing HMS partitions with
`ignore_if_exists`, preserving idempotent append behavior.

Decision: Partition recovery should discover directories through the configured
object-store registry for non-file URI table locations, with local filesystem
scanning reserved for bare local and `file:` paths.

Rationale: HMS table locations are not guaranteed to be local paths. Spark's
interop contract is the partition metadata visible through HMS, so recovery
must not silently skip `s3://`, `abfs://`, `gs://`, or another registered object
store just because it is not representable as a local path. A focused Rust test
uses an in-memory object store to validate Spark-style escaped partition
prefixes, raw logical partition values, and escaped partition locations without
paying for the full HMS/Spark harness.

Decision: HMS schema conversion preserves Spark timestamp LTZ and NTZ metadata
when Spark's data-source schema JSON is available.

Rationale: Spark stores both Hive column type strings and data-source schema
JSON. Hive's `timestamp` type alone cannot distinguish LTZ from NTZ, so Sail
uses Spark's JSON metadata to restore `timestamp` versus `timestamp_ntz`, and
writes the same distinction for Spark to restore Sail-created tables. The
interop tests validate schema shape and string-cast values in both directions.

## Interop Validation Backlog

Keep non-location HMS/Spark discrepancies visible here until they are either
covered by focused tests or intentionally deferred.

- Table classification: managed tables should remain managed, and explicit
  `LOCATION` tables should restore as external Spark data-source tables.
- Storage metadata shape: Spark data-source tables need SerDe/storage
  property `path`, compatible input/output formats, and compatible SerDe class
  metadata, not only a readable filesystem directory.
- User-visible properties: raw `spark.sql.*`, Hive DDL timestamps, and Spark
  datasource bookkeeping should stay filtered while still reconstructing
  parsed table fields.
- Schema and format restoration: Spark-created and Sail-created tables should
  agree on provider, column names, column types, and nullability for supported
  types.
- Timestamp schema restoration: Spark-created and Sail-created `TIMESTAMP` and
  `TIMESTAMP_NTZ` columns are covered in both directions. The contract is schema
  restoration and stable string-cast values, not a full time-zone semantics
  matrix.
- Partition metadata: partition columns, HMS partition entries, and partition
  locations are covered for space-containing and slash-containing values in
  Spark-to-Sail and Sail-to-Spark partitioned Parquet interop.
- Partition value escaping: Spark writes slash-containing partition values as
  percent-escaped path segments (for example `region=a%2Fb`) and restores the
  logical value as `a/b`. Sail-created HMS metadata now registers raw logical
  partition values with escaped locations so reference Spark restores `a/b`,
  and Sail decodes Spark/Hive escaped listing partition path segments before
  exposing row values or evaluating filters.
- Object-store partition recovery: non-file URI partition discovery is covered
  by a focused Rust object-store test. The live HMS/Spark Python harness still
  uses local Docker-visible paths, so S3/ABFS/GCS credential and service
  compatibility remains a separate environment matrix rather than assumed from
  this suite.
- Alter-location behavior: Spark-to-Sail and Sail-to-Spark tests cover
  `ALTER TABLE ... SET LOCATION` by checking restored location metadata and
  readability of the new path only.
- Database semantics: relative database `LOCATION` and relative table
  `LOCATION` should continue matching Spark's warehouse/database qualification
  behavior.
- Cache behavior: `REFRESH TABLE` is not required for the current roundtrips,
  but stale metadata or negative lookup caching should be captured with a
  focused repro if it appears.
- Benign Spark probes: reference Spark may log missing `_spark_metadata`
  warnings while creating data-source tables at new explicit locations. Record
  this only when Sail's observable behavior diverges, not for Spark's expected
  probe noise.

## Regenerating GSSAPI Bindings

The GSSAPI FFI bindings in `crates/sail-catalog-hms/src/security/gssapi_bindings.rs` were generated by `bindgen` from the system GSSAPI headers. To regenerate for a newer GSSAPI version:

```bash
bindgen /usr/include/gssapi.h -o crates/sail-catalog-hms/src/security/gssapi_bindings.rs
```

The generated bindings are checked in because the build environment may not have `bindgen` installed. The bindings are portable across architectures (x86_64, ARM, macOS, Linux) because the GSSAPI C ABI uses only standard types (`u32` constants, `*mut c_void` pointers, `usize` lengths) that adapt to the host automatically.

## Guidance for Future Changes

If you extend HMS support, keep these boundaries explicit in docs and reviews:

1. Kerberos auth support is not the same as transactional Hive support.
2. Passing the current Kerberos harness means the auth and metadata path works, not the full HMS surface.
3. Compatibility with older Hive or vendor-specific HMS deployments should be treated as a versioned test matrix problem, not assumed from Apache Hive success.

The most likely next areas of work are:

- broader distro/version compatibility testing
- explicit investigation of transactional HMS APIs
- deciding whether the reusable Kerberos KDC harness should move into a shared test utility crate if another consumer, such as HDFS integration tests, needs it
