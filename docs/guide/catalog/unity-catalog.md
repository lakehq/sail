---
title: Unity Catalog
rank: 3
---

# Unity Catalog

The Unity Catalog provider in Sail allows you to connect to an external catalog that exposes the [Unity Catalog API](https://docs.unitycatalog.io/).

Unity Catalog can be configured using the following options:

CHECK HERE

- `type` (required): The string `unity`.
- `name` (required): The name of the catalog.
- `uri` (optional): The base URI of the Unity Catalog API. Defaults to `http://localhost:8080/api/2.1/unity-catalog`.
- `default_catalog` (optional): The default catalog name to use. Defaults to `unity`.
- `token` (optional): The authentication token to use when connecting to the Unity Catalog API. Defaults to empty (no authentication). Refer to the [Unity Catalog Auth Docs](https://docs.unitycatalog.io/server/auth/) for more information.

## Example

```bash
export SAIL_CATALOG__LIST='[{type="unity", name="sail", uri="https://catalog.example.com", default_catalog="meow", token="..."}]'
```
