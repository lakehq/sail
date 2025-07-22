---
title: Azure Storage
rank: 5
---

# Azure Storage

Sail supports both Azure Blob Storage and Azure Data Lake Storage for reading and writing data.

## URI Formats

### Azure Blob Storage

```
az://container/path
azure://container/path
https://account.blob.core.windows.net/container/path
https://account.blob.fabric.microsoft.com/container/path
```

### ADLS Gen2

```
abfs://container@account.dfs.core.windows.net/path
abfss://container@account.dfs.core.windows.net/path
https://account.dfs.core.windows.net/container/path
https://account.dfs.fabric.microsoft.com/container/path
```

### Legacy Format

```
adl://path  # ADLS Gen1 - deprecated
```
