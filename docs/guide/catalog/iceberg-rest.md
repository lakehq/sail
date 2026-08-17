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
