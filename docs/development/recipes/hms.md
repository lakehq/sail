---
title: Hive Metastore
---

# Hive Metastore Development Notes

This page documents the current HMS implementation and test strategy in Sail for contributors and maintainers.

## Maintainer Summary

The current HMS work provides a metadata-focused compatibility layer for Apache Hive Metastore.

What maintainers can rely on today:

- Plain HMS catalog connectivity over Thrift.
- Kerberos-secured HMS connectivity over Thrift SASL.
- Kerberos SASL frame wrapping (`auth-int` / `auth-conf`) with configurable minimum QOP (`min_sasl_qop`).
- HMS endpoint lists with automatic failover on retryable transport/Thrift errors.
- Basic metadata CRUD for databases and tables through `HmsCatalogProvider`.
- Spark datasource v2 table interop for tables registered in HMS: Sail reads `parquet`, `csv`, `json`, `avro`, and `delta`. Sail writes `parquet`, `csv`, `json`, and `delta`; Avro write is not implemented yet. Sail resolves the table location from Spark's datasource metadata.
- Iceberg-in-HMS tables: Sail detects HMS-registered Iceberg tables, resolves their metadata location, reads via the Iceberg table provider, and commits new snapshots under a per-table HMS lock with compare-and-swap precondition checking (`crates/sail-catalog-hms/src/managed_table.rs`).
- A self-contained Kerberos integration harness built with `testcontainers`.
- A DSV2 + Iceberg format interop harness (`python/pysail/tests/spark/catalog/hms/`) that round-trips tables between JVM Spark and Sail through a real HMS + MinIO.
- CI wiring for the Rust ignored/Kerberos and Python HMS/Spark catalog-test lanes.

What maintainers should not infer from the current test suite:

- Broad production readiness across HMS distributions and versions.
- Support for Hive ACID or transactional HMS APIs.
- Support for delegation tokens or TLS.
- ORC engine support: ORC is detected at the HMS metadata layer (including Spark `USING ORC` tables, via SerDe fallback) but Sail has no ORC reader. A Sail catalog accepts `USING ORC` for metadata purposes but cannot read the files.

## What Green Means

There are two green targets, each proving a different layer.

### Kerberos transport + basic metadata (ignored harness)

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

This is evidence that the Kerberos transport and basic HMS metadata path work end to end.

It is not evidence that Sail supports:

- Hive ACID transaction APIs
- non-Apache HMS distributions
- production RDBMS-backed HMS deployments

It is only partial evidence for HA behavior:

- the current test suite covers ordered endpoint failover and retryable transport errors
- it does not yet prove broader operational behavior such as DNS churn under long-lived production traffic, vendor-specific HA frontends, or multi-region deployment patterns

### Format interop (DSV2 + Iceberg)

The format interop harness target is:

```bash
hatch run pytest -m integration python/pysail/tests/spark/catalog/hms -v
```

When this target is green, it means Sail successfully:

- round-trips `parquet`, `csv`, `json`, `avro` tables between JVM Spark and Sail through a real HMS + MinIO (Spark writes, Sail reads for all four; Sail writes, Spark reads for `parquet`, `csv`, `json`)
- round-trips `delta` datasource tables (Spark writes, Sail reads)
- round-trips Iceberg tables in both directions through HMS, exercising the `managed_table.rs` commit path
- advances the Iceberg metadata pointer on Sail insert and rejects stale commits
- preserves decimal/timestamp type fidelity, column comments, foreign table properties, and identity-partition pruning across the DSV2 formats

This is evidence that the Spark datasource location handling in `sail_catalog_hms::convert` and the Iceberg managed-table commit machinery both work end to end against a real Hive Metastore and a reference JVM Spark.

It is not evidence that:

- ORC tables are readable (Sail has no ORC reader; ORC is metadata-detected only)
- Sail can write Avro (the Sail-create/Spark-read Avro cases are xfailed: "Writer not implemented for this format"; Spark-written Avro is read back fine)
- Hive `textfile` tables are readable (the format is metadata-detected, but Sail has no `textfile`/`LazySimpleSerDe` file reader yet; the textfile interop test is xfailed)
- complex/nested types round-trip for DSV2 formats (the parquet `schema_matrix` and `mixed_complex_partitioned` tests are xfailed as "not yet working in Hive 4")

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

The catalog-test trigger runs both the Rust ignored/Kerberos harness and the Python HMS/Spark interop lane.

On pull requests, that lane is enabled when either:

- the PR has the `run catalog tests` label
- the head commit message contains `[catalog test]` or `[catalog tests]`

On pushes to `main`, the catalog-test lane runs automatically.

Relevant workflow locations:

- `.github/workflows/build.yml` — gates and dispatches the catalog-test lane
- `.github/workflows/rust-tests.yml` — the Rust ignored/Kerberos lane; installs Kerberos runtime/client packages before running ignored tests so the host-side `kinit` step and Sail's dynamic GSSAPI loading both work in CI
- `.github/workflows/catalog-tests.yml` — the Python HMS/Spark interop lane; runs the installed-package format-interop harness (`hatch run pytest -m integration --pyargs pysail.tests.spark -v`)

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
- Spark datasource v2 table resolution: Sail recognizes Spark-registered datasource tables, resolves the Spark-recorded table location, and routes to the matching file-format reader (`parquet`, `csv`, `json`, `avro`, `delta`)
- Unsupported Spark datasource providers do not fail metadata conversion. Sail classifies the table from its HMS storage metadata (SerDe/InputFormat/OutputFormat); when that detection is inconclusive, the declared provider string is preserved (lower-cased) rather than collapsed to `unknown`. Listing and describe therefore remain usable even when reading the files is unsupported
- Iceberg-in-HMS table lifecycle: recognize HMS-registered Iceberg tables, resolve their current metadata file, read via the Iceberg table provider, and commit new snapshots under a per-table HMS lock with compare-and-swap precondition checking

Security guarantees in this contract:

- Downgrade fail-fast: if the configured `min_sasl_qop` cannot be met by the server SASL layer advertisement, transport creation fails before the HMS client is usable.
- Session-wide enforcement: when `auth_int` or `auth_conf` is negotiated, every subsequent Thrift frame on that connection is wrapped on write and unwrapped on read.

Out of scope for the current implementation:

- transactional HMS methods such as `open_txns`, `lock`, `heartbeat`, `allocate_table_write_ids`, or compaction APIs
- Hortonworks or other distribution-specific compatibility promises
- automatic keytab management inside Sail
- ORC as an engine format: ORC is detected at the HMS metadata layer but Sail has no ORC reader; `USING ORC` is accepted for metadata but the files cannot be read

## Table Format & Metadata Contract

The auth/transport contract above covers _how Sail talks to HMS_. A separate
contract covers _what Sail writes to HMS_ for each table format, and how it reads
it back. This is the layer most contributors will actually touch.

Spark retrofitted HMS to behave like its catalog abstraction. The compatibility
contract is whatever Spark has historically tolerated, and it drifts across
Spark versions. It is **discovered, not specified**: location mirroring, Iceberg
metadata pointer handling, schema storage, provider normalization, and fallback
behavior were learned from bugs, user reports, and interop runs rather than from
a stable protocol document.

Because this contract drifts, maintainer docs stay behavioral. They describe
what Sail supports and where to add coverage; they do not duplicate the exact
HMS property names or storage-shape details from the implementation.

Test ownership for this contract:

- **Conversion rules** are pinned by the constants and unit tests in
  `crates/sail-catalog-hms/src/convert.rs` (e.g. `build_generic_table`,
  `table_to_status`, `inject_spark_metadata`). These gate every PR and prove the
  pure conversion logic. When metadata encoding changes, change the code and its
  tests together.
- **Cross-engine behavior** — does JVM Spark round-trip a table Sail wrote, and
  vice versa, through a real HMS + MinIO? — is pinned by the format interop
  harness in `python/pysail/tests/spark/catalog/hms/`. This is the only layer
  that surfaces contract drift against a real Spark.
- **When Spark drift is discovered**, add or adjust an executable test first (a
  unit test for the pure conversion rule, and/or an interop test for the
  cross-engine behavior), then make the code pass. Document the supported
  behavior and test ownership, not a prose copy of the metadata encoding.
  Interop discovers; the unit suite encodes the regression guard.

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
