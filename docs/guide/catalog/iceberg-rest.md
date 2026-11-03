---
title: Iceberg REST
rank: 2
---

# Iceberg REST Catalog

The Iceberg REST catalog provider in Sail allows you to connect to an external catalog that exposes the [Iceberg REST Catalog API](https://iceberg.apache.org/rest-catalog-spec/).

## Options

An Iceberg REST catalog can be configured using the following options.

- `type` (required): The catalog provider. Set this option to `iceberg-rest`.
- `name` (required): The catalog name.
- `uri` (required): The base URI of the Iceberg REST catalog server.
- `warehouse` (optional): The catalog warehouse location.
- `prefix` (optional): The prefix for catalog API endpoints.
- `namespace_separator` (optional): The client-side fallback for the Iceberg REST `namespace-separator` catalog property. Use the separator that the REST server expects, such as `::` or `/`. Sail also accepts URL-encoded values returned by REST servers, such as `%3A%3A` for `::`. If this option is not set or is empty, Sail uses the Iceberg REST default unit separator.
- `oauth_access_token` (optional): The OAuth 2.0 access token.
- `bearer_access_token` (optional): The bearer token for authentication. This option takes precedence over `oauth_access_token`.
- `bearer_access_token_file` (optional): The path to a file that holds the bearer token. This option takes precedence over `bearer_access_token` and `oauth_access_token`. Sail reads the token from this file for every request, so it picks up a rotated token, such as a kubelet-projected service account token, without restarting the server. If a request is rejected with `401 Unauthorized`, Sail reloads the file and retries the request once. The file must contain the raw token without the `Bearer ` prefix for the HTTP `Authorization` header. Surrounding whitespace in the file is trimmed. Empty or unreadable files produce an error rather than falling back to other options.

See [Common Options](./index.md#common-options) for caching configuration.

## Server Configuration

Sail calls `GET /v1/config` before catalog operations. The final Iceberg REST catalog configuration is merged in this order:

1. Server `defaults`.
2. Sail client configuration.
3. Server `overrides`.

Server overrides take precedence over matching values configured in Sail. For example, if the REST server returns `namespace-separator` in `overrides`, Sail uses that value to encode multipart namespaces even when `namespace_separator` is configured locally. Configuring `namespace_separator` in Sail does not configure the REST server. The server must already decode the same separator, usually by advertising it from `/v1/config`.

## Examples

This example configures an Iceberg REST catalog without authentication.

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com"}]'
```

This example uses an OAuth access token.

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", oauth_access_token="..."}]'
```

This example uses a bearer token.

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token="..."}]'
```

This example reads a bearer token from a file, such as a kubelet-projected service account token.

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token_file="/var/run/secrets/tokens/catalog-token"}]'
```

This example configures a client-side namespace separator fallback.

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", namespace_separator="::"}]'
```

## Session Catalogs

A Spark session can also register an Iceberg REST catalog at runtime, with the Spark catalog options `spark.sql.catalog.<name>`. Set them with `spark.conf.set(...)` (the Spark Connect `Config` operation) or with the SQL `SET` statement.

- `spark.sql.catalog.<name>`: set this to `iceberg-rest`.
- `spark.sql.catalog.<name>.uri` (required): the base URI of the catalog server.
- `spark.sql.catalog.<name>.warehouse`, `.prefix`, `.namespace_separator`, `.oauth_access_token` and `.bearer_access_token`: the same options as above.

```python
spark.conf.set("spark.sql.catalog.org_1", "iceberg-rest")
spark.conf.set("spark.sql.catalog.org_1.uri", "https://catalog.example.com")
spark.conf.set("spark.sql.catalog.org_1.warehouse", "org_1")
spark.conf.set("spark.sql.catalog.org_1.bearer_access_token", "...")
spark.sql("SELECT * FROM org_1.raw.events")
```

The catalog is registered once its type and `uri` are set, and it is replaced when one of its options changes. Unsetting `spark.sql.catalog.<name>` drops it. A change that cannot be applied is rejected and leaves the options as they were.

- The catalog belongs to the session. Other sessions do not see it, and it is dropped when the session is released. A session cannot replace a catalog from the server configuration or the `system` catalog, and the default catalog cannot be dropped. Unsetting an option of a catalog that the session did not register does nothing.
- A session catalog has no catalog cache.
- The `bearer_access_token_file` option is not available, since the server would read a file named by the client.
- The values of options that match `spark.redaction.regex` (by default `secret`, `password`, `token` and `access key` in the key) are redacted from `GetAll` results, the result of `SET`, and the debug log of configuration requests. The text of a SQL `SET` statement is still part of the debug log of the statement.
- A catalog type other than `iceberg-rest` is stored as a configuration value only.

## Table DDL

See the [Iceberg DDL support matrix](./index.md#iceberg) for supported operations.

- Registering an existing table requires the server's register-table endpoint and access to the table's metadata file.
- Format upgrades require server support for the target version. Column defaults require Iceberg format version 3 and typed literals.
- `DROP TABLE ... PURGE` requests deletion of the table's data. Support and deletion behavior depend on the catalog server.
