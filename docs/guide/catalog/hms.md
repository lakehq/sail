---
title: Hive Metastore
rank: 6
---

# Hive Metastore

The Hive Metastore (HMS) catalog provider connects Sail to an external Hive Metastore service over Thrift. It lets Sail use the databases, tables, and views whose metadata is stored in that service.

Sail supports plain Thrift connections, Kerberos-protected Thrift SASL connections, and high-availability endpoint lists. It supports flat database namespaces and resolves the provider and location recorded for existing HMS tables. See [Data Sources](../sources/) for the formats Sail can read and write.

Sail does not support Hive ACID operations, including transaction heartbeats, locks, and write ID allocation. It also does not support delegation-token authentication.

## Options

An HMS catalog can be configured using the following options.

- `type` (required): The catalog provider. Set this option to `hive_metastore` or `hms`.
- `name` (required): The catalog name.
- `uris` (required): The HMS endpoints. Provide a list whose entries use either `host:port` or `thrift://host:port`. An entry can contain a comma-separated list of endpoints.
- `thrift_transport` (optional): The Thrift transport mode. Valid values are `buffered` and `framed`. The default is `buffered`.
- `auth` (optional): The HMS authentication mode. Valid values are `none` and `kerberos`. The default is `none`.
- `kerberos_service_principal` (optional): The HMS service principal. Set this option when `auth = "kerberos"`. Use the form `service/_HOST@REALM`, such as `hive-metastore/_HOST@EXAMPLE.COM`.
- `min_sasl_qop` (optional): The minimum Kerberos SASL quality of protection. Set this option when `auth = "kerberos"`. Valid values are `auth`, `auth_int`, and `auth_conf`. The default is `auth`.
- `connect_timeout_secs` (optional): The connection timeout for each endpoint. Set this option in seconds. The default is `5`.

See [Common Options](./index.md#common-options) for options that configure caching.

## Endpoint Failover Behavior

Sail tries endpoints in the order in which they are configured. It resolves the selected endpoint's DNS name for each new connection, so connections do not remain pinned to the address found at startup. When a retryable transport or Thrift error occurs, Sail moves to the next endpoint. If a connection fails after a create or drop request may have succeeded, Sail treats the resulting `AlreadyExists` or `NotFound` response as a successful retry.

## Kerberos Authentication

::: info
Sail uses the same Kerberos operating model for Hive Metastore and HDFS.
:::

### Prerequisites

- The Hive Metastore service must be configured for Kerberos.
- The Sail server host must have a valid `krb5.conf` file.
- The Sail server process must have a valid Kerberos ticket cache.
- The Sail server host must have Kerberos runtime libraries. On Linux, Sail loads `libgssapi_krb5.so.2` at runtime. On macOS, you can install them with `brew install krb5`.

### Starting the Sail Server

Run `kinit` before starting the Sail server so that the server process can use its ticket cache.

```python
import subprocess
from pysail.spark import SparkConnectServer

# authenticate with Kerberos
subprocess.run([
    "kinit",
    "-kt",
    "/path/to/user.keytab",
    "username@YOUR.REALM",
], check=True)

# start the Sail server
server = SparkConnectServer(ip="0.0.0.0", port=50051)
server.start(background=False)
```

::: tip
In a distributed deployment, every worker needs its own Kerberos credentials.
:::

### Kerberos Catalog Configuration

When `auth = "kerberos"`, Sail replaces `_HOST` in `kerberos_service_principal` with the hostname of the endpoint selected for that connection attempt.

Sail fails the connection when the server cannot meet `min_sasl_qop`. Once it negotiates `auth_int` or `auth_conf`, it protects every Thrift frame for that connection with the Kerberos SASL security layer.

Sail uses the existing ticket cache and does not run `kinit` or manage keytabs. It does not use delegation tokens or transactional HMS APIs. The integration is intended for metadata operations rather than Hive ACID write coordination.

## Table Types

HMS records whether a table is managed or external in its `table_type` metadata. For tables created by Spark, tables created without `LOCATION` appear as `MANAGED`, and tables created with `LOCATION` appear as `EXTERNAL`.

Sail always creates tables as external by marking them as `EXTERNAL` and setting `table_type` to `EXTERNAL_TABLE`. For tables created by other engines, Sail reports the type stored in HMS.

When Sail drops an HMS table, it removes only the metadata. It does not ask HMS to delete the table's data, regardless of the table type.

## Examples

The following example configures a single unencrypted HMS endpoint.

```bash
export SAIL_CATALOG__LIST='[{type="hive_metastore", name="sail", uris=["127.0.0.1:9083"]}]'
```

This example uses two endpoints, a framed Thrift transport, and a ten-second connection timeout.

```bash
export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["hms1.internal:9083","hms2.internal:9083"], thrift_transport="framed", connect_timeout_secs=10}]'
```

This example connects to an HMS service with Kerberos authentication and requires integrity protection for the connection.

```bash
export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["hms.internal:9083"], auth="kerberos", kerberos_service_principal="hive-metastore/_HOST@EXAMPLE.COM", min_sasl_qop="auth_int", thrift_transport="framed"}]'
```

This example enables shared caching for database and table listings.

```bash
export SAIL_CATALOG__LIST='[{type="hms", name="sail", uris=["127.0.0.1:9083"], database_cache_type="global", database_cache_ttl_secs=3600, table_cache_type="global", table_cache_size=1000}]'
```
