---
title: Iceberg REST
rank: 2
---

# Iceberg REST Catalog

The Iceberg REST catalog provider in Sail allows you to connect to an external catalog that exposes the [Iceberg REST Catalog API](https://iceberg.apache.org/rest-catalog-spec/).

An Iceberg REST catalog can be configured using the following options:

- `name` (required): The name of the catalog.
- `type` (required): The string `iceberg-rest`.
- `uri` (required): The base URI of the Iceberg REST catalog server.
- `warehouse` (optional): The warehouse location for the catalog.
- `prefix` (optional): The prefix for all catalog API endpoints.
- `oauth_access_token` (optional): The OAuth 2.0 access token.
- `bearer_access_token` (optional): The bearer token for authentication.

## Examples

```bash
export SAIL_CATALOG__LIST='[{name="sail", type="iceberg-rest", uri="https://catalog.example.com"}]'

# OAuth authentication
export SAIL_CATALOG__LIST='[{name="sail", type="iceberg-rest", uri="https://catalog.example.com", warehouse="s3://data/warehouse", oauth_access_token="..."}]'

# Bearer token authentication
export SAIL_CATALOG__LIST='[{name="sail", type="iceberg-rest", uri="https://catalog.example.com", warehouse="s3://data/warehouse", bearer_access_token="..."}]'
```
