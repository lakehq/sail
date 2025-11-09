---
title: Unity Catalog
rank: 3
---

# Unity Catalog

The Unity Catalog provider in Sail allows you to connect to an external catalog that exposes the [Unity Catalog API](https://docs.unitycatalog.io/).

Unity Catalog can be configured using the following options:

- `type` (required): The string `unity`.
- `name` (required): The name of the catalog.
- `uri` (optional): The base URI of the Unity Catalog API. Defaults to `http://localhost:8080/api/2.1/unity-catalog`.
- `default_catalog` (optional): The default catalog name to use. Defaults to `unity`.
- `token` (optional): The authentication token to use when connecting to the Unity Catalog API. Defaults to empty (no authentication). Refer to the [Unity Catalog Auth Docs](https://docs.unitycatalog.io/server/auth/) for more information.

## Environment Variables

Sail automatically loads Unity Catalog configuration from environment variables with the following prefixes:

- `DATABRICKS_*` - Databricks-specific configuration
- `UNITY_*` - Unity Catalog-specific configuration
- `UC_*` - Short-form Unity Catalog configuration

The following table lists the supported configuration keys and their corresponding environment variable names:

| Configuration        | Supported Keys                                                                                        |
| -------------------- | ----------------------------------------------------------------------------------------------------- |
| Host/URI             | DATABRICKS_HOST, DATABRICKS_WORKSPACE_URL, UNITY_HOST, UNITY_WORKSPACE_URL, UC_HOST, UC_WORKSPACE_URL |
| Token                | DATABRICKS_TOKEN, DATABRICKS_ACCESS_TOKEN, UNITY_ACCESS_TOKEN, UC_ACCESS_TOKEN                        |
| Client ID            | DATABRICKS_CLIENT_ID, UNITY_CLIENT_ID, UC_CLIENT_ID                                                   |
| Client Secret        | DATABRICKS_CLIENT_SECRET, UNITY_CLIENT_SECRET, UC_CLIENT_SECRET                                       |
| Authority ID         | DATABRICKS_AUTHORITY_ID, UNITY_AUTHORITY_ID, UC_AUTHORITY_ID                                          |
| Authority Host       | DATABRICKS_AUTHORITY_HOST, UNITY_AUTHORITY_HOST, UC_AUTHORITY_HOST                                    |
| MSI Endpoint         | DATABRICKS_MSI_ENDPOINT, UNITY_MSI_ENDPOINT, UC_MSI_ENDPOINT                                          |
| MSI Resource ID      | DATABRICKS_MSI_RESOURCE_ID, UNITY_MSI_RESOURCE_ID, UC_MSI_RESOURCE_ID                                 |
| Object ID            | DATABRICKS_OBJECT_ID, UNITY_OBJECT_ID, UC_OBJECT_ID                                                   |
| Federated Token File | DATABRICKS_FEDERATED_TOKEN_FILE, UNITY_FEDERATED_TOKEN_FILE, UC_FEDERATED_TOKEN_FILE                  |
| Use Azure CLI        | DATABRICKS_USE_AZURE_CLI, UNITY_USE_AZURE_CLI, UC_USE_AZURE_CLI                                       |
| Allow HTTP URL       | DATABRICKS_ALLOW_HTTP_URL, UNITY_ALLOW_HTTP_URL, UC_ALLOW_HTTP_URL                                    |

## Example

```bash
export UNITY_CLIENT_SECRET='...'
export SAIL_CATALOG__LIST='[{type="unity", name="sail", uri="https://catalog.example.com", default_catalog="meow", token="..."}]'
```
