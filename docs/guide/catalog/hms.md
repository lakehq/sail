---
title: Hive Metastore
rank: 6
---

# Hive Metastore

The Hive Metastore catalog provider in Sail allows you to connect to an external Hive Metastore service over Thrift.

## Support Status

Sail's HMS integration is currently aimed at metadata interoperability with Apache Hive Metastore deployments.

The following areas are supported:

- Plain HMS connections over Thrift.
- Kerberos-secured HMS connections over Thrift SASL.
- HMS high-availability URI lists with endpoint failover.
- Flat database namespaces.
- Database, table, and view metadata stored in HMS.
- Spark datasource v2 formats registered in HMS (`parquet`, `csv`, `json`, `avro`, `delta`): Sail resolves the table location from Spark's datasource metadata. Sail reads all five formats. Sail writes `parquet`, `csv`, `json`, and `delta`; Avro write is not implemented yet (Sail-create/Spark-read Avro is tracked as a known gap).
- Iceberg tables registered in HMS: Sail resolves the Iceberg metadata location, reads via the Iceberg table provider, and commits new snapshots under a per-table lock with compare-and-swap precondition checking.
- Hive native `textfile` tables: Sail detects the format at the HMS metadata layer (SerDe library string matching) and resolves the table location from the storage descriptor, but Sail has no `textfile`/`LazySimpleSerDe` file reader yet, so the files cannot be read until the reader and format mapping are implemented.

The following areas are not implemented yet:

- Hive ACID or transactional HMS APIs such as transaction heartbeats, locks, or write ID allocation.
- ORC as a first-class engine format: ORC tables are detected at the HMS metadata layer (including Spark `USING ORC` tables, via SerDe fallback) but Sail has no ORC file reader, so an ORC table registered in HMS cannot be read by Sail today. ORC is intentionally excluded from the Spark datasource provider set in `sail_catalog_hms::convert`.
- Delegation-token authentication.

Hive Metastore can be configured using the following options:

- `type` (required): The string `hive_metastore` or the alias `hms`.
- `name` (required): The name of the catalog.
- `uris` (required): A list of HMS endpoints. Each entry accepts either `host:port` or `thrift://host:port`. Entries may also include comma-separated endpoint lists.
- `thrift_transport` (optional): The Thrift transport mode. Valid values are `buffered` and `framed`. The default is `buffered`.
- `auth` (optional): The HMS authentication mode. Valid values are `none` and `kerberos`. The default is `none`.
- `kerberos_service_principal` (optional): Required when `auth = "kerberos"`. Use the HMS service principal in the form `service/_HOST@REALM`, for example `hive-metastore/_HOST@EXAMPLE.COM`.
- `min_sasl_qop` (optional): Minimum Kerberos SASL QOP when `auth = "kerberos"`. Valid values are `auth`, `auth_int`, and `auth_conf`. The default is `auth`.
- `connect_timeout_secs` (optional): Per-endpoint connect timeout in seconds. The default is `5`.

See [Common Options](./index.md#common-options) for caching configuration.

Failover behavior:

- Sail attempts endpoints in configured order.
- New connections re-resolve DNS for the selected endpoint instead of pinning the initial startup address forever.
- Retryable transport/Thrift failures rotate to the next endpoint.
- A retried create or drop normalizes `AlreadyExists` and `NotFound` responses when the prior attempt likely succeeded but the response was lost.
- Per-endpoint connect timeout defaults to `5s` and can be overridden with `connect_timeout_secs`.

## Kerberos Authentication

::: info
Kerberos authentication for Hive Metastore is supported and uses the same operator model as Sail's HDFS support.
:::

### Prerequisites

- A Kerberos-enabled Hive Metastore service.
- A valid `krb5.conf` file on the Sail server host.
- A valid Kerberos ticket cache for the Sail server process.
- Kerberos runtime libraries on the Sail server host.
  On Linux Sail loads `libgssapi_krb5.so.2` at runtime.
  On macOS install Kerberos libraries, for example with `brew install krb5`.

### Starting the Sail Server

Authenticate with Kerberos before starting the Sail server.

```python
import subprocess
from pysail.spark import SparkConnectServer

# authenticate with Kerberos
subprocess.run([
    "kinit", "-kt",
    "/path/to/user.keytab",
    "username@YOUR.REALM"
], check=True)

# start the Sail server
server = SparkConnectServer(ip="0.0.0.0", port=50051)
server.start(background=False)
```

::: tip
The Sail server uses the process ticket cache created by `kinit`.

If you run Sail in a distributed environment, each worker needs its own Kerberos credentials.
:::

### Kerberos HMS Catalog Configuration

When `auth = "kerberos"` is enabled, Sail expands `_HOST` in `kerberos_service_principal` from the hostname of the endpoint selected for that connection attempt.

```bash
export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["hms1.internal:9083","thrift://hms2.internal:9083"], auth="kerberos", kerberos_service_principal="hive-metastore/_HOST@EXAMPLE.COM"}]'
```

### Security Guarantees

- Downgrade fail-fast: if `min_sasl_qop` cannot be satisfied by the server-advertised SASL layers, connection setup fails immediately.
- Session-wide protection: once a wrapped QOP (`auth_int` or `auth_conf`) is negotiated, every Thrift frame for that connection is wrapped/unwrapped through the Kerberos SASL security layer.

### Current Limitations

- Sail uses an existing Kerberos ticket cache. It does not run `kinit` or manage keytabs internally.
- Delegation-token authentication is not supported.
- Transactional Hive Metastore APIs are not used yet. Sail currently targets metadata CRUD rather than Hive ACID write coordination.

## Table Types

Sail distinguishes between **managed** and **external** tables based on the `table_type` field stored in HMS metadata:

- **Managed tables** (created without `LOCATION`, e.g. by Spark) report `Type: MANAGED` in `DESCRIBE EXTENDED`.
- **External tables** (created with `LOCATION` or by Sail itself) report `Type: EXTERNAL`.

For `DROP TABLE`, Sail uses metadata-only semantics for HMS and does **not** request physical data deletion via the HMS `delete_data` flag, regardless of managed vs external type.

Sail always creates its own tables as external (`EXTERNAL=TRUE`, `table_type = EXTERNAL_TABLE`). When reading tables created by other engines (e.g. Spark), Sail reflects the type recorded in HMS.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="hive_metastore", name="sail", uris=["127.0.0.1:9083"]}]'

export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["hms1.internal:9083","hms2.internal:9083"], thrift_transport="framed", connect_timeout_secs=10}]'

export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["hms.internal:9083"], auth="kerberos", kerberos_service_principal="hive-metastore/_HOST@EXAMPLE.COM", min_sasl_qop="auth_int", thrift_transport="framed"}]'

# Enabling caching for database and table listings
export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["127.0.0.1:9083"], database_cache_type="global", database_cache_ttl_secs=3600, table_cache_type="global", table_cache_size=1000}]'
```
