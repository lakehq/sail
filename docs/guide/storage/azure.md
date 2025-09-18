---
title: Azure
rank: 5
---

# Azure

Sail supports reading from and writing to Azure storage services.

## URI Formats

Sail supports the following URI formats for Azure storage services:

<ul>
  <li>
    Azure protocol
    <ul>
      <li>
        <code><SyntaxText raw="'azure://'<container>'/'<path>" /></code>
      </li>
    </ul>
  </li>
  <li>
    AZ protocol
    <ul>
      <li>
        <code><SyntaxText raw="'az://'<container>'/'<path>" /></code>
      </li>
    </ul>
  </li>
  <li>
    Azure Blob File System (ABFS) and secure ABFS (ABFSS) protocols
    <ul>
      <li>
        <code><SyntaxText raw="'abfs'['s']'://'<container>'/'<path>" /></code>
        (fsspec convention)
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'abfs'['s']'://'<container>'@'<account>'.dfs.core.windows.net/'<path>"
        /></code>
        (Hadoop driver convention)
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'abfs'['s']'://'<container>'@'<account>'.dfs.fabric.windows.net/'<path>"
        /></code>
        (Hadoop driver convention with Microsoft Fabric)
      </li>
    </ul>
  </li>
  <li>
    ADL protocol
    <ul>
      <li>
        <code><SyntaxText raw="'adl://'<container>'/'<path>" /></code>
      </li>
    </ul>
  </li>
  <li>
    HTTPS endpoints
    <ul>
      <li>
        <code
          ><SyntaxText
            raw="'https://'<account>'.dfs.core.windows.net/'<container>'/'<path>"
        /></code>
        (Azure Data Lake Storage Gen2)
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'https://'<account>'.blob.core.windows.net/'<container>'/'<path>"
        /></code>
        (Azure Blob Storage)
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'https://'<account>'.dfs.fabric.microsoft.com/'<workspace>'/'<path>"
        /></code>
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'https://'<account>'.blob.fabric.microsoft.com/'<workspace>'/'<path>"
        /></code>
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'https://onelake.dfs.fabric.microsoft.com/'<workspace>'/'<item>'.'<item-type>'/'<path>"
        /></code>
      </li>
      <li>
        <code
          ><SyntaxText
            raw="'https://onelake.dfs.fabric.microsoft.com/'<workspace-guid>'/'<item-guid>'/'<path>"
        /></code>
      </li>
    </ul>
  </li>
</ul>

## Configuration

You can use environment variables to configure Azure storage services in Sail.
Some configuration options can be set using different environment variables.

::: warning
The environment variables to configure Azure storage services are experimental and may change in future versions of Sail.
:::

### Security Configuration

- `AZURE_STORAGE_ACCOUNT_NAME`

  The name of the Azure storage account, if not specified in the URI.

- `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_ACCESS_KEY`, `AZURE_STORAGE_MASTER_KEY`

  The master key for accessing the storage account.

- `AZURE_STORAGE_CLIENT_ID`, `AZURE_CLIENT_ID`

  The service principal client ID for authorizing requests.

- `AZURE_STORAGE_CLIENT_SECRET`, `AZURE_CLIENT_SECRET`

  The service principal client secret for authorizing requests.

- `AZURE_STORAGE_TENANT_ID`, `AZURE_STORAGE_AUTHORITY_ID`, `AZURE_TENANT_ID`, `AZURE_AUTHORITY_ID`

  The tenant ID used in OAuth flows.

- `AZURE_STORAGE_AUTHORITY_HOST`, `AZURE_AUTHORITY_HOST`

  The authority host used in OAuth flows.

- `AZURE_STORAGE_SAS_KEY`, `AZURE_STORAGE_SAS_TOKEN`

  The shared access signature (SAS) key or token. The signature should be percent-encoded.

- `AZURE_STORAGE_TOKEN`

  The bearer token for authorizing requests.

- `AZURE_MSI_ENDPOINT`, `AZURE_IDENTITY_ENDPOINT`

  The endpoint for acquiring a managed identity token.

- `AZURE_OBJECT_ID`

  The object ID for use with managed identity authentication.

- `AZURE_MSI_RESOURCE_ID`

  The MSI resource ID for use with managed identity authentication.

- `AZURE_FEDERATED_TOKEN_FILE`

  The file containing a token for Azure AD workload identity federation.

- `AZURE_USE_AZURE_CLI`

  Whether to use the Azure CLI for acquiring an access token.

- `AZURE_SKIP_SIGNATURE`

  Whether to skip signing requests.

### Storage Configuration

- `AZURE_CONTAINER_NAME`

  The name of the Azure storage container, if not specified in the URI.

- `AZURE_STORAGE_ENDPOINT`, `AZURE_ENDPOINT`

  The endpoint used to communicate with blob storage. This overrides the default endpoint.

- `AZURE_USE_FABRIC_ENDPOINT`

  Whether to use the Microsoft Fabric URL scheme.

- `AZURE_DISABLE_TAGGING`

  Whether to disable tagging objects.

- `AZURE_STORAGE_USE_EMULATOR`, `AZURE_USE_EMULATOR`

  Whether to use the Azurite storage emulator.

### Microsoft Fabric Configuration

- `AZURE_FABRIC_TOKEN_SERVICE_URL`

  The URL for the Fabric token service.

- `AZURE_FABRIC_WORKLOAD_HOST`

  The host for the Fabric workload.

- `AZURE_FABRIC_SESSION_TOKEN`

  The session token for Fabric.

- `AZURE_FABRIC_CLUSTER_IDENTIFIER`

  The cluster identifier for Fabric.

::: info
For configuration options that accept boolean values, you can specify `1`, `true`, `on`, `yes`, or `y` for a true value, and specify `0`, `false`, `off`, `no`, or `n` for a false value.
The boolean values are case-insensitive.
:::

## Examples

<!--@include: ../_common/spark-session.md-->

### Spark DataFrame API

```python
# You can use any valid URI format for Azure storage services
# to specify the path to read or write data.
path = "azure://my-container/path/to/data"

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema="id INT, name STRING")
df.write.parquet(path)

df = spark.read.parquet(path)
df.show()
```

### Spark SQL

```python
# You can use any valid URI format for Azure storage services
# to specify the location of the table.
sql = """
CREATE TABLE my_table (id INT, name STRING)
USING parquet
LOCATION 'azure://my-container/path/to/data'
"""
spark.sql(sql)
spark.sql("SELECT * FROM my_table").show()

spark.sql("INSERT INTO my_table VALUES (3, 'Charlie'), (4, 'David')")
spark.sql("SELECT * FROM my_table").show()
```

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
