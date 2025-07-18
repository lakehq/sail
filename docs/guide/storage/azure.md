---
title: Azure Storage
rank: 5
---

# Azure Storage

Sail supports both Azure Blob Storage and Azure Data Lake Storage (ADLS) Gen2 for reading and writing data.

## URL Formats

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

## Azure Blob Storage vs ADLS Gen2

### When to Use Azure Blob Storage

- General-purpose object storage
- Unstructured data (images, videos, backups)
- Simple storage needs without complex permissions
- Cost-optimized storage tiers

### When to Use ADLS Gen2

- Big data analytics workloads
- Hierarchical file system requirements
- Fine-grained ACLs and POSIX permissions
- High-performance directory operations
- Data lake architectures

Performance comparison:
| Operation | Blob Storage | ADLS Gen2 |
|-----------|--------------|-----------|
| Object read/write | Excellent | Excellent |
| Directory listing | Slow (simulated) | Fast (native) |
| Rename operations | Slow (copy+delete) | Fast (atomic) |
| Permission management | Container-level | File/directory-level |

## Authentication

### Account Key

```bash
# Basic authentication
export AZURE_STORAGE_ACCOUNT_NAME="your_account_name"
export AZURE_STORAGE_ACCOUNT_KEY="your_account_key"

# Using connection string
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
```

### Service Principal

```bash
# Using service principal
export AZURE_TENANT_ID="your_tenant_id"
export AZURE_CLIENT_ID="your_client_id"
export AZURE_CLIENT_SECRET="your_client_secret"
```

### Managed Identity

```python
# Automatically uses managed identity when running on Azure
# No explicit credentials needed
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/path")
```

### SAS Token

```bash
# Using SAS token
export AZURE_STORAGE_SAS_TOKEN="?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx..."

# Or append to URL
df = spark.read.parquet("https://account.blob.core.windows.net/container/path?sv=2020-08-04...")
```

## Reading Data

### Basic Reading

```python
# Read various formats
df_parquet = spark.read.parquet("azure://container/data.parquet")
df_csv = spark.read.option("header", "true").csv("azure://container/data.csv")
df_json = spark.read.json("azure://container/data.json")
df_orc = spark.read.orc("azure://container/data.orc")

# ADLS Gen2 with full path
df = spark.read.parquet("abfss://container@account.dfs.core.windows.net/data/")
```

### Partitioned Data

```python
# Read partitioned dataset
# Structure: /data/year=2024/month=01/day=15/
df = spark.read.parquet("azure://container/data/")

# Filter partitions
df = spark.read.parquet("azure://container/data/year=2024/month=*/")

# Multiple paths
df = spark.read.parquet([
    "azure://container/data/2023/",
    "azure://container/data/2024/"
])
```

### Wildcards and Patterns

```python
# Using wildcards
df = spark.read.parquet("azure://container/logs/2024-*.parquet")

# Recursive file lookup
df = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("azure://container/nested-data/")

# Schema evolution
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("azure://container/evolving-schema/")
```

## Writing Data

### Basic Writing

```python
# Write modes
df.write.mode("overwrite").parquet("azure://container/output/")
df.write.mode("append").parquet("azure://container/output/")
df.write.mode("ignore").parquet("azure://container/output/")
df.write.mode("error").parquet("azure://container/output/")  # Default

# With compression
df.write \
    .option("compression", "snappy") \
    .parquet("azure://container/compressed/")
```

### Partitioned Writing

```python
# Partition by columns
df.write \
    .partitionBy("year", "month", "day") \
    .parquet("azure://container/partitioned/")

# Dynamic partition overwrite (ADLS Gen2)
df.write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date") \
    .parquet("abfss://container@account.dfs.core.windows.net/data/")

# Control file sizes
df.repartition(10).write.parquet("azure://container/ten-files/")
df.coalesce(1).write.parquet("azure://container/single-file/")
```

### Format-Specific Options

```python
# CSV with options
df.write \
    .option("header", "true") \
    .option("delimiter", "|") \
    .option("quote", "\"") \
    .csv("azure://container/data.csv")

# JSON with pretty print
df.write \
    .option("pretty", "true") \
    .json("azure://container/data.json")
```

## Working with SQL

```sql
-- Create external table on Blob Storage
CREATE TABLE IF NOT EXISTS blob_table
USING parquet
OPTIONS (
  path 'azure://container/data/'
)
LOCATION 'azure://container/data/';

-- Create table on ADLS Gen2
CREATE TABLE IF NOT EXISTS adls_table
USING parquet
LOCATION 'abfss://container@account.dfs.core.windows.net/data/';

-- Query with partition pruning
SELECT * FROM adls_table
WHERE year = 2024 AND month IN (1, 2, 3);

-- CTAS with partitioning
CREATE TABLE azure_summary
USING parquet
LOCATION 'azure://container/summary/'
PARTITIONED BY (year, month)
AS SELECT
  year, month,
  product_id,
  SUM(sales) as total_sales
FROM blob_table
GROUP BY year, month, product_id;
```

## Common Use Cases

### Data Lake Architecture

```python
# Bronze layer - raw data ingestion
raw_data = spark.readStream \
    .format("json") \
    .load("/streaming/source/")

raw_data.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "abfss://lake@account.dfs.core.windows.net/checkpoints/bronze/") \
    .partitionBy("date") \
    .start("abfss://lake@account.dfs.core.windows.net/bronze/events/")

# Silver layer - cleaned and validated
bronze_df = spark.read.parquet("abfss://lake@account.dfs.core.windows.net/bronze/events/")
silver_df = bronze_df \
    .dropDuplicates(["event_id"]) \
    .filter(col("is_valid") == True) \
    .withColumn("processed_time", current_timestamp())

silver_df.write \
    .mode("overwrite") \
    .partitionBy("date", "event_type") \
    .parquet("abfss://lake@account.dfs.core.windows.net/silver/events/")

# Gold layer - business aggregates
gold_df = spark.sql("""
    SELECT
        date,
        event_type,
        COUNT(*) as event_count,
        SUM(amount) as total_amount
    FROM silver_events
    GROUP BY date, event_type
""")

gold_df.write \
    .mode("overwrite") \
    .parquet("abfss://lake@account.dfs.core.windows.net/gold/daily_summary/")
```

### Cross-Region Replication

```python
# Read from primary region
df = spark.read.parquet("abfss://container@account-eastus.dfs.core.windows.net/data/")

# Process data
processed = df.transform(complex_processing)

# Write to secondary region for DR
processed.write \
    .mode("overwrite") \
    .parquet("abfss://container@account-westus.dfs.core.windows.net/backup/")
```

### Hybrid Cloud Pattern

```python
# Read from on-premises
on_prem_df = spark.read.parquet("hdfs://namenode:9000/data/")

# Enrich with cloud data
azure_ref = spark.read.parquet("azure://reference-data/lookup/")
enriched = on_prem_df.join(azure_ref, ["key"], "left")

# Write results to Azure
enriched.write \
    .partitionBy("date") \
    .parquet("azure://processed/enriched-data/")
```

## Performance Optimization

### Connection Pooling

```python
# Optimize connection settings
spark.conf.set("spark.hadoop.fs.azure.max.connections", "100")
spark.conf.set("spark.hadoop.fs.azure.connection.timeout", "300000")
spark.conf.set("spark.hadoop.fs.azure.read.timeout", "60000")
```

### Block Size Configuration

```python
# Set optimal block size for large files
spark.conf.set("spark.hadoop.fs.azure.block.size", "268435456")  # 256MB

# Adjust read buffer size
spark.conf.set("spark.hadoop.fs.azure.read.buffer.size", "4194304")  # 4MB
```

### Parallelism Settings

```python
# Increase parallelism for large datasets
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Use adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## Storage Tiers

### Using Different Access Tiers

```python
# Write to cool tier for archival
df.write \
    .option("fs.azure.storage.tier", "Cool") \
    .parquet("azure://archive/historical-data/")

# Lifecycle management via configuration
spark.conf.set("fs.azure.lifecycle.rule.days", "30")
spark.conf.set("fs.azure.lifecycle.rule.action", "TierToCool")
```

## Security Best Practices

### 1. Use Managed Identities

```python
# Preferred for Azure-hosted Spark
# No credentials in code
df = spark.read.parquet("abfss://secure@account.dfs.core.windows.net/data/")
```

### 2. Implement Network Security

```bash
# Restrict to VNet
# Configure Private Endpoints
# Use Service Endpoints
```

### 3. Enable Encryption

```python
# Data is encrypted at rest by default
# Enable encryption in transit
spark.conf.set("fs.azure.secure.mode", "true")
```

### 4. Audit Access

```python
# Enable diagnostic logging
spark.conf.set("fs.azure.enable.diagnostics", "true")
```

## Local Testing with Azurite

### Setup

```bash
# Start Azurite
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite

# Configure for local testing
export AZURE_STORAGE_ACCOUNT_NAME="devstoreaccount1"
export AZURE_STORAGE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
export AZURE_STORAGE_ENDPOINT="http://localhost:10000/devstoreaccount1"
export AZURE_STORAGE_USE_EMULATOR="true"
```

### Create Test Container

```python
from azure.storage.blob import BlobServiceClient

connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_service_client.create_container("test")
```

::: warning
Azurite only emulates Blob Storage. ADLS Gen2 features (hierarchical namespace, ACLs) are not available in local testing.
:::

## Troubleshooting

### Authentication Failures

```python
# Debug authentication
import os
print(f"Account: {os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', 'Not set')}")
print(f"Has key: {'AZURE_STORAGE_ACCOUNT_KEY' in os.environ}")
print(f"Has connection string: {'AZURE_STORAGE_CONNECTION_STRING' in os.environ}")

# Test connection
try:
    spark.read.text("azure://test/test.txt").show()
except Exception as e:
    print(f"Error: {e}")
```

### Container Not Found

```bash
# List containers
az storage container list --account-name myaccount

# Create container if missing
az storage container create --name mycontainer --account-name myaccount
```

### Performance Issues

```python
# Enable metrics
spark.conf.set("spark.hadoop.fs.azure.enable.metrics", "true")

# Check partition skew
df.rdd.glom().map(len).collect()

# Monitor task execution
spark.sparkContext.statusTracker().getActiveJobIds()
```

### Network Timeouts

```python
# Increase timeouts
spark.conf.set("spark.hadoop.fs.azure.connection.timeout", "300000")  # 5 minutes
spark.conf.set("spark.hadoop.fs.azure.read.timeout", "300000")

# Retry configuration
spark.conf.set("spark.hadoop.fs.azure.retry.limit", "5")
spark.conf.set("spark.hadoop.fs.azure.retry.interval", "1000")
```

### ADLS Gen2 Permission Errors

```bash
# Check ACLs
az storage fs access show \
  --path /data \
  --file-system myfilesystem \
  --account-name myaccount

# Set ACLs
az storage fs access set \
  --acl "user::rwx,group::r-x,other::---" \
  --path /data \
  --file-system myfilesystem \
  --account-name myaccount
```

## Best Practices

1. **Choose the Right Storage Type**
   - Use ADLS Gen2 for analytics workloads
   - Use Blob Storage for object storage needs
2. **Optimize File Layout**

   - Target 100-200 MB file sizes
   - Avoid deep directory nesting in Blob Storage
   - Use partitioning wisely (not too granular)

3. **Manage Costs**

   - Use lifecycle policies for old data
   - Choose appropriate storage tiers
   - Monitor transaction costs

4. **Ensure Reliability**
   - Use zone-redundant storage (ZRS) for critical data
   - Implement retry logic
   - Handle transient failures gracefully
