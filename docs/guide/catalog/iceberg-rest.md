---
title: Iceberg REST
rank: 2
---

# Iceberg REST Catalog

The Iceberg REST catalog provider in Sail allows you to connect to an external catalog that exposes the [Iceberg REST Catalog API](https://iceberg.apache.org/rest-catalog-spec/).

An Iceberg REST catalog can be configured using the following options:

- `type` (required): The string `iceberg-rest`.
- `name` (required): The name of the catalog.
- `uri` (required): The base URI of the Iceberg REST catalog server.
- `warehouse` (optional): The warehouse location for the catalog.
- `prefix` (optional): The prefix for all catalog API endpoints.
- `namespace_separator` (optional): A client-side fallback for the Iceberg REST `namespace-separator` catalog property.
  Use the separator string the REST server expects, such as `::` or `/`.
  Sail also accepts URL-encoded values returned by REST servers, such as `%3A%3A` for `::`.
  If unset or empty, Sail uses the Iceberg REST default unit separator.
- `oauth_access_token` (optional): The OAuth 2.0 access token.
- `bearer_access_token` (optional): The bearer token for authentication.

See [Common Options](./index.md#common-options) for caching configuration.

## Server Configuration

Sail calls `GET /v1/config` before catalog operations. The final Iceberg REST catalog configuration is merged in this order:

1. Server `defaults`
2. Sail client configuration
3. Server `overrides`

Server overrides take precedence over matching values configured in Sail. For example, if the REST server returns `namespace-separator` in `overrides`, Sail uses that value to encode multipart namespaces even when `namespace_separator` is configured locally. Configuring `namespace_separator` in Sail does not configure the REST server; the server must already decode the same separator, usually by advertising it from `/v1/config`.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com"}]'

# OAuth authentication
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", oauth_access_token="..."}]'

# Bearer token authentication
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token="..."}]'

# Client-side namespace separator fallback
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", namespace_separator="::"}]'
```
