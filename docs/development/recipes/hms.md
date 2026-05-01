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

Keep this log focused on non-obvious current contracts, test-harness choices,
and intentional deferrals. When behavior simply matches Spark and is covered by
tests, remove it from this section instead of preserving old implementation
history here.

### 2026-04-28: Internal Spark and HMS bookkeeping stays out of user properties

Context: Spark data-source tables store provider, schema, partition, bucket,
sort, and statistics metadata in HMS table parameters under `spark.sql.*`, and
Hive may add `transient_lastDdlTime`.

Decision: Sail consumes supported internal metadata to reconstruct table
status, but filters those bookkeeping keys from user-visible properties.

Consequence: Interop tests should validate parsed fields such as provider,
schema, partitioning, sorting, and statistics rather than asserting that raw
Spark or Hive bookkeeping keys remain visible.

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

## Current Interop Coverage

The current Python HMS roundtrip suite plus focused Rust tests cover:

- managed versus explicit-`LOCATION` table restoration in both directions
- relative database `LOCATION` and relative table `LOCATION` qualification
- provider, schema, format, and nullability restoration for supported types
- `TIMESTAMP` and `TIMESTAMP_NTZ` restoration in both directions
- partition metadata, escaped partition values, and partition discovery
- non-location alter-table rewrites and `ALTER TABLE ... SET LOCATION`

## Interop Validation Backlog

Keep only real gaps or intentionally deferred environment coverage here.

- Live object-store matrix: partition recovery for non-file URIs is covered by
  a focused Rust object-store test, but the Python HMS/Spark harness still uses
  local Docker-visible paths. Real S3/ABFS/GCS service and credential coverage
  remains a separate environment matrix.
- Reference Spark Connect coverage: the interop harness still uses a classic
  JVM `SparkSession` as the reference Spark side. Coverage with a second Spark
  Connect server remains deferred.
- Cache behavior: `REFRESH TABLE` is not required for the current roundtrips,
  but stale metadata or negative lookup caching should be captured with a
  focused repro if it appears.

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
