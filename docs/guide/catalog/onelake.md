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
- `bearer_token` (optional): The bearer token for authentication.

The `url` should be in the format <code><SyntaxText raw="<workspace>'/'<item-name>'.'<item-type>" /></code>.

If `bearer_token` is not provided, Sail will attempt to find credentials from the following sources in order:

1. The `AZURE_STORAGE_TOKEN` environment variable.
2. The `AZURE_ACCESS_TOKEN` environment variable.
3. Azure CLI (`az account get-access-token`).

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse"}]'

# Bearer token authentication
export SAIL_CATALOG__LIST='[{type="onelake", name="fabric", url="workspace/lakehouse.Lakehouse", bearer_token="..."}]'
```

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
