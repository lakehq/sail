---
title: Azure Storage
rank: 5
---

# Azure Storage

Sail supports reading from and writing to Azure storage services.

## URI Formats

Sail supports the following access patterns for Azure and Azure-compatible object storage:

### Azure protocol:

- `azure://`

### AZ protocol:

- `az://`

### ABFS protocol:

- `abfs://`

### ABFSS protocol (secure):

- `abfss://`

### HTTPS endpoints:

- `https://account-name.dfs.core.windows.net/container-name/path`
- `https://account-name.blob.core.windows.net/container-name/path`
- `https://account-name.dfs.fabric.microsoft.com/workspace/path`
- `https://account-name.blob.fabric.microsoft.com/workspace/path`

### ADL protocol:

- `adl://`

## Azure Protocol

`azure://container-name/path/to/object`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("azure://my-container/path")
df = spark.read.parquet("azure://my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE azure_table
USING parquet
LOCATION 'azure://my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM azure_table;").show()

spark.sql("INSERT INTO azure_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM azure_table;").show()
```

## AZ Protocol

`az://container-name/path/to/object`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("az://my-container/path")
df = spark.read.parquet("az://my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE az_table
USING parquet
LOCATION 'az://my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM az_table;").show()

spark.sql("INSERT INTO az_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM az_table;").show()
```

## ABFS Protocol

### FSSpec Convention

`abfs://container-name/path/to/object`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("abfs://my-container/path")
df = spark.read.parquet("abfs://my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE abfs_table
USING parquet
LOCATION 'abfs://my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM abfs_table;").show()

spark.sql("INSERT INTO abfs_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM abfs_table;").show()
```

### Hadoop Driver Convention

`abfs://file-system@account-name.dfs.core.windows.net/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("abfs://myfilesystem@myaccount.dfs.core.windows.net/path")
df = spark.read.parquet("abfs://myfilesystem@myaccount.dfs.core.windows.net/path").show()
```

### SQL

```python
sql = """
CREATE TABLE abfs_hadoop_table
USING parquet
LOCATION 'abfs://myfilesystem@myaccount.dfs.core.windows.net/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM abfs_hadoop_table;").show()

spark.sql("INSERT INTO abfs_hadoop_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM abfs_hadoop_table;").show()
```

## ABFSS Protocol (Secure)

### FSSpec Convention

`abfss://container-name/path/to/object`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("abfss://my-container/path")
df = spark.read.parquet("abfss://my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE abfss_table
USING parquet
LOCATION 'abfss://my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM abfss_table;").show()

spark.sql("INSERT INTO abfss_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM abfss_table;").show()
```

### Hadoop Driver Convention

`abfss://file-system@account-name.dfs.core.windows.net/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("abfss://myfilesystem@myaccount.dfs.core.windows.net/path")
df = spark.read.parquet("abfss://myfilesystem@myaccount.dfs.core.windows.net/path").show()
```

### SQL

```python
sql = """
CREATE TABLE abfss_hadoop_table
USING parquet
LOCATION 'abfss://myfilesystem@myaccount.dfs.core.windows.net/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM abfss_hadoop_table;").show()

spark.sql("INSERT INTO abfss_hadoop_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM abfss_hadoop_table;").show()
```

## HTTPS Endpoints

### Azure Data Lake Storage Gen2 (DFS)

`https://account-name.dfs.core.windows.net/container-name/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("https://myaccount.dfs.core.windows.net/my-container/path")
df = spark.read.parquet("https://myaccount.dfs.core.windows.net/my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE dfs_https_table
USING parquet
LOCATION 'https://myaccount.dfs.core.windows.net/my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM dfs_https_table;").show()

spark.sql("INSERT INTO dfs_https_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM dfs_https_table;").show()
```

### Azure Blob Storage

`https://account-name.blob.core.windows.net/container-name/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("https://myaccount.blob.core.windows.net/my-container/path")
df = spark.read.parquet("https://myaccount.blob.core.windows.net/my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE blob_https_table
USING parquet
LOCATION 'https://myaccount.blob.core.windows.net/my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM blob_https_table;").show()

spark.sql("INSERT INTO blob_https_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM blob_https_table;").show()
```

### Microsoft Fabric OneLake (DFS)

`https://account-name.dfs.fabric.microsoft.com/workspace/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("https://onelake.dfs.fabric.microsoft.com/workspace-guid/item-guid/Files/path")
df = spark.read.parquet("https://onelake.dfs.fabric.microsoft.com/workspace-guid/item-guid/Files/path").show()
```

### SQL

```python
sql = """
CREATE TABLE fabric_dfs_table
USING parquet
LOCATION 'https://onelake.dfs.fabric.microsoft.com/workspace-guid/item-guid/Files/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM fabric_dfs_table;").show()

spark.sql("INSERT INTO fabric_dfs_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM fabric_dfs_table;").show()
```

### Microsoft Fabric OneLake (Blob)

`https://account-name.blob.fabric.microsoft.com/workspace/path`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("https://onelake.blob.fabric.microsoft.com/workspace/item.itemtype/path")
df = spark.read.parquet("https://onelake.blob.fabric.microsoft.com/workspace/item.itemtype/path").show()
```

### SQL

```python
sql = """
CREATE TABLE fabric_blob_table
USING parquet
LOCATION 'https://onelake.blob.fabric.microsoft.com/workspace/item.itemtype/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM fabric_blob_table;").show()

spark.sql("INSERT INTO fabric_blob_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM fabric_blob_table;").show()
```

## ADL Protocol

`adl://container-name/path/to/object`

### DataFrame

```python
spark.sql("SELECT 1").write.parquet("adl://my-container/path")
df = spark.read.parquet("adl://my-container/path").show()
```

### SQL

```python
sql = """
CREATE TABLE adl_table
USING parquet
LOCATION 'adl://my-container/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM adl_table;").show()

spark.sql("INSERT INTO adl_table VALUES (3, 'Charlie'), (4, 'Jessica');")
spark.sql("SELECT * FROM adl_table;").show()
```
