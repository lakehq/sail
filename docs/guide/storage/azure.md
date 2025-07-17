---
title: Azure Storage
rank: 5
---

# Azure Storage

Sail supports both Azure Blob Storage and Azure Data Lake Storage (ADLS) Gen2 for reading and writing data.

## URL Formats

Azure storage supports multiple URL schemes:

- `az://container/path` - Generic Azure storage URL
- `azure://container/path` - Alias for Azure storage
- `abfs://container@account.dfs.core.windows.net/path` - ADLS Gen2 (unencrypted)
- `abfss://container@account.dfs.core.windows.net/path` - ADLS Gen2 (encrypted)
- `adl://path` - Legacy ADLS Gen1 (deprecated)

You can also use HTTPS URLs that are automatically recognized:

- `https://account.blob.core.windows.net/container/path`
- `https://account.dfs.core.windows.net/container/path`
- `https://account.blob.fabric.microsoft.com/container/path`
- `https://account.dfs.fabric.microsoft.com/container/path`

## Authentication

Azure storage authentication is configured through environment variables:

```bash
export AZURE_STORAGE_ACCOUNT_NAME="your_account_name"
export AZURE_STORAGE_ACCOUNT_KEY="your_account_key"
```

For ADLS Gen2, you can also use:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
```

## Azure Blob Storage vs ADLS Gen2

### Azure Blob Storage

- Object storage for unstructured data
- Flat namespace (directories are virtual)
- Access via `azure://` or `az://` URLs
- Good for general-purpose storage

### ADLS Gen2

- Built on Blob Storage with hierarchical namespace
- True directory structure with POSIX permissions
- Access via `abfs://` or `abfss://` URLs
- Optimized for big data analytics workloads
- Better performance for directory operations

## Examples

### Reading Data

```python
# Azure Blob Storage
df = spark.read.parquet("azure://mycontainer/data/file.parquet")

# ADLS Gen2 (secure)
df = spark.read.parquet("abfss://mycontainer@myaccount.dfs.core.windows.net/data/file.parquet")

# Using HTTPS URL
df = spark.read.parquet("https://myaccount.blob.core.windows.net/mycontainer/data/file.parquet")
```

### Writing Data

```python
# Azure Blob Storage
df.write.parquet("azure://mycontainer/output/")

# ADLS Gen2
df.write.parquet("abfss://mycontainer@myaccount.dfs.core.windows.net/output/")
```

## Local Testing

For local development, you can use [Azurite](https://github.com/Azure/Azurite) to emulate Azure Blob Storage. Note that Azurite does not support ADLS Gen2 features.

```bash
export AZURE_STORAGE_ACCOUNT_NAME="devstoreaccount1"
export AZURE_STORAGE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
export AZURE_STORAGE_ENDPOINT="http://localhost:10000/devstoreaccount1"
export AZURE_STORAGE_USE_EMULATOR="true"
```

::: info
When using Azurite for local testing, only `azure://` and `az://` URL schemes are supported. ADLS Gen2 specific features (hierarchical namespace, POSIX permissions) are not available.
:::
