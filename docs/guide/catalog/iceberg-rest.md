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
- `namespace_separator` (optional): The URL-encoded separator for multipart namespace path parameters.
  This corresponds to the Iceberg REST `namespace-separator` catalog property.
  If unset, Sail uses the Iceberg REST default unit separator (`\x1F`).
- `oauth_access_token` (optional): The OAuth 2.0 access token.
- `bearer_access_token` (optional): The bearer token for authentication.

See [Common Options](./index.md#common-options) for caching configuration.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com"}]'

# OAuth authentication
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", oauth_access_token="..."}]'

# Bearer token authentication
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token="..."}]'

# Custom multipart namespace separator
export SAIL_CATALOG__LIST='[{type="iceberg-rest", name="sail", uri="https://catalog.example.com", namespace_separator="%1F"}]'
```
