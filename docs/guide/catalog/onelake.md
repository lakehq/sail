---
title: OneLake
rank: 5
---

# Microsoft OneLake

The OneLake catalog provider in Sail allows you to connect to [Microsoft Fabric OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview).

OneLake catalog can be configured using the following options:

- `type` (required): The string `onelake`.
- `name` (required): The name of the catalog.
- `url` (required): The OneLake item location.
- `api` (optional): The OneLake catalog API. Use `delta` for the Unity Catalog endpoint or `iceberg` for the Iceberg REST catalog endpoint. The default is `iceberg`.
- `bearer_token` (optional): The bearer token for authentication.

See [Common Options](./index.md#common-options) for caching configuration.

The `url` should be in the format <code><SyntaxText raw="<workspace>'/'<item-name>'.'<item-type>" /></code>.

If `bearer_token` is not specified, Sail creates credentials from environment variables, similar to how authentication is handled in [Azure storage](../storage/azure.md).
These credentials acquire Microsoft Entra tokens for the Azure Storage audience required by OneLake.

The following environment variables are supported:

- `AZURE_STORAGE_TOKEN`, `AZURE_ACCESS_TOKEN`

  The bearer token for authorizing requests.

- `AZURE_STORAGE_CLIENT_ID`, `AZURE_CLIENT_ID`

  The service principal client ID for authorizing requests. This can also identify a user-assigned managed identity.

- `AZURE_STORAGE_CLIENT_SECRET`, `AZURE_CLIENT_SECRET`

  The service principal client secret for authorizing requests.

- `AZURE_STORAGE_TENANT_ID`, `AZURE_STORAGE_AUTHORITY_ID`, `AZURE_TENANT_ID`, `AZURE_AUTHORITY_ID`

  The tenant ID used in OAuth flows.

- `AZURE_STORAGE_AUTHORITY_HOST`, `AZURE_AUTHORITY_HOST`

  The authority host used in OAuth flows.

- `AZURE_MSI_ENDPOINT`, `AZURE_IDENTITY_ENDPOINT`

  The endpoint for acquiring a managed identity token.

- `AZURE_OBJECT_ID`

  The object ID for use with managed identity authentication.

- `AZURE_MSI_RESOURCE_ID`

  The MSI resource ID for use with managed identity authentication.

- `AZURE_FEDERATED_TOKEN_FILE`

  The file containing a token for Azure AD workload identity federation.

- `AZURE_USE_AZURE_CLI`

  Whether to use Azure CLI for acquiring an access token.

Tokens are refreshed automatically before expiry.

::: warning
Azure account keys and SAS tokens are not supported for OneLake catalog authentication. OneLake requires Microsoft Entra bearer tokens.
:::

## Examples

```bash
# Unity Catalog endpoint
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse", api="delta"}]'

# Iceberg REST catalog endpoint
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse", api="iceberg"}]'

# Bearer token authentication
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse", bearer_token="..."}]'

# OAuth authentication with a tenant ID
export AZURE_TENANT_ID="..."
export AZURE_CLIENT_ID="..."
export AZURE_CLIENT_SECRET="..."
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse"}]'

# Azure CLI authentication via environment variables
export AZURE_USE_AZURE_CLI=true
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse"}]'
```

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
