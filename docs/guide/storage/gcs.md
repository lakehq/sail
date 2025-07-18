---
title: Google Cloud Storage
rank: 6
---

# Google Cloud Storage

Sail supports reading from and writing to Google Cloud Storage (GCS) buckets using the `gs://` URI scheme.

## URI Format

```
gs://bucket-name/path/to/object
```

Components:

- `gs://` - The GCS protocol scheme
- `bucket-name` - The name of your GCS bucket
- `/path/to/object` - The path to the object within the bucket

Examples:

```python
# Single file
df = spark.read.parquet("gs://my-bucket/data/file.parquet")

# Directory/prefix
df = spark.read.parquet("gs://my-bucket/data/2024/")

# With wildcards
df = spark.read.parquet("gs://my-bucket/data/*/part-*.parquet")
```

## Authentication

### Application Default Credentials (Recommended)

```bash
# Authenticate using gcloud CLI
gcloud auth application-default login

# Or set service account key file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### Service Account Key

```bash
# Using file path
export GOOGLE_SERVICE_ACCOUNT="/path/to/service-account-key.json"

# Or embed JSON directly
export GOOGLE_SERVICE_ACCOUNT_KEY='{
  "type": "service_account",
  "project_id": "your-project",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  ...
}'
```

### Workload Identity (GKE)

```python
# Automatically uses workload identity when running on GKE
# No explicit credentials needed
df = spark.read.parquet("gs://my-bucket/data/")
```

### Service Account Impersonation

```bash
# Impersonate a service account
export GOOGLE_IMPERSONATE_SERVICE_ACCOUNT="service-account@project.iam.gserviceaccount.com"
```

## Reading Data

### Basic Reading

```python
# Read various formats
df_parquet = spark.read.parquet("gs://bucket/data.parquet")
df_csv = spark.read.option("header", "true").csv("gs://bucket/data.csv")
df_json = spark.read.json("gs://bucket/data.json")
df_orc = spark.read.orc("gs://bucket/data.orc")
df_avro = spark.read.format("avro").load("gs://bucket/data.avro")

# Read with options
df = spark.read \
    .option("mergeSchema", "true") \
    .option("recursiveFileLookup", "true") \
    .parquet("gs://bucket/data/")
```

### Partitioned Data

```python
# Read partitioned dataset
# GCS structure: gs://bucket/data/year=2024/month=01/day=15/
df = spark.read.parquet("gs://bucket/data/")

# Filter partitions while reading
df = spark.read.parquet("gs://bucket/data/year=2024/month=*/")

# Read specific partitions
df = spark.read.parquet([
    "gs://bucket/data/year=2024/month=01/",
    "gs://bucket/data/year=2024/month=02/"
])
```

### Reading Multiple Files

```python
# Read with glob patterns
df = spark.read.parquet("gs://bucket/logs/2024-*.parquet")

# Read from multiple buckets
df = spark.read.parquet([
    "gs://bucket1/data/",
    "gs://bucket2/data/"
])

# Handle schema evolution
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("gs://bucket/evolving-schema/")
```

## Writing Data

### Basic Writing

```python
# Write modes
df.write.mode("overwrite").parquet("gs://bucket/output/")
df.write.mode("append").parquet("gs://bucket/output/")
df.write.mode("ignore").parquet("gs://bucket/output/")
df.write.mode("error").parquet("gs://bucket/output/")  # Default

# Write with compression
df.write \
    .option("compression", "snappy") \
    .parquet("gs://bucket/compressed/")
```

### Partitioned Writing

```python
# Partition by columns
df.write \
    .partitionBy("year", "month", "day") \
    .parquet("gs://bucket/partitioned/")

# Dynamic partition overwrite
df.write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("date") \
    .parquet("gs://bucket/data/")

# Control output files
df.repartition(100).write.parquet("gs://bucket/many-files/")
df.coalesce(1).write.parquet("gs://bucket/single-file/")
```

### Output Formats

```python
# Various formats with options
df.write.parquet("gs://bucket/data.parquet")

df.write \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv("gs://bucket/data.csv")

df.write \
    .option("pretty", "true") \
    .json("gs://bucket/data.json")

df.write.orc("gs://bucket/data.orc")
df.write.format("avro").save("gs://bucket/data.avro")
```

## Working with SQL

```sql
-- Create external table
CREATE TABLE IF NOT EXISTS gcs_table
USING parquet
OPTIONS (path 'gs://bucket/data/')
LOCATION 'gs://bucket/data/';

-- Query with partition pruning
SELECT * FROM gcs_table
WHERE year = 2024 AND month = 1;

-- Create view for complex queries
CREATE OR REPLACE VIEW gcs_view AS
SELECT
  year, month,
  product_id,
  SUM(sales) as total_sales
FROM gcs_table
GROUP BY year, month, product_id;

-- CTAS with partitioning
CREATE TABLE gcs_aggregated
USING parquet
LOCATION 'gs://bucket/aggregated/'
PARTITIONED BY (year, month)
AS SELECT * FROM gcs_view;
```

## Common Use Cases

### Data Lake Architecture

```python
# Bronze layer - raw ingestion
raw_df = spark.readStream \
    .format("json") \
    .load("gs://streaming-source/events/")

raw_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "gs://lake/checkpoints/bronze/") \
    .partitionBy("date") \
    .start("gs://lake/bronze/events/")

# Silver layer - cleaned data
bronze_df = spark.read.parquet("gs://lake/bronze/events/")
silver_df = bronze_df \
    .dropDuplicates(["event_id"]) \
    .filter(col("is_valid") == True) \
    .withColumn("processed_time", current_timestamp())

silver_df.write \
    .mode("overwrite") \
    .partitionBy("date", "event_type") \
    .parquet("gs://lake/silver/events/")

# Gold layer - business metrics
gold_df = spark.sql("""
    SELECT
        date,
        event_type,
        COUNT(*) as event_count,
        SUM(revenue) as total_revenue
    FROM silver_events
    GROUP BY date, event_type
""")

gold_df.write \
    .mode("overwrite") \
    .parquet("gs://lake/gold/daily_metrics/")
```

### Multi-Region Data Processing

```python
# Read from multiple regions
regions = ["us-central1", "europe-west1", "asia-southeast1"]
dfs = []

for region in regions:
    df = spark.read.parquet(f"gs://data-{region}/sales/")
    df = df.withColumn("region", lit(region))
    dfs.append(df)

# Combine and process
global_df = dfs[0].unionByName(*dfs[1:])
summary = global_df.groupBy("region", "product_id") \
    .agg(sum("sales").alias("total_sales"))

# Write to global bucket
summary.write \
    .mode("overwrite") \
    .partitionBy("region") \
    .parquet("gs://global-analytics/sales-summary/")
```

### ML Pipeline

```python
# Read training data
train_df = spark.read.parquet("gs://ml-data/training/")

# Feature engineering
from pyspark.ml.feature import VectorAssembler, StandardScaler

assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"],
    outputCol="features"
)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# Process and save features
features_df = assembler.transform(train_df)
scaled_df = scaler.fit(features_df).transform(features_df)

scaled_df.write \
    .mode("overwrite") \
    .parquet("gs://ml-data/processed-features/")

# Save model artifacts
model_path = "gs://ml-models/my-model/v1/"
# Model saving code here
```

## Performance Optimization

### Connection Settings

```python
# Optimize GCS connector
spark.conf.set("spark.hadoop.fs.gs.http.max.retry", "10")
spark.conf.set("spark.hadoop.fs.gs.http.connect-timeout", "20000")
spark.conf.set("spark.hadoop.fs.gs.http.read-timeout", "20000")

# Connection pooling
spark.conf.set("spark.hadoop.fs.gs.http.max.connections", "100")
```

### Read Performance

```python
# Enable GCS connector optimizations
spark.conf.set("spark.hadoop.fs.gs.inputstream.fast.fail.on.not.found", "true")
spark.conf.set("spark.hadoop.fs.gs.inputstream.support.gzip.encoding", "true")

# Prefetch configuration
spark.conf.set("spark.hadoop.fs.gs.inputstream.prefetch.enabled", "true")
spark.conf.set("spark.hadoop.fs.gs.inputstream.prefetch.buffer.size", "8388608")  # 8MB
```

### Write Performance

```python
# Optimize write buffer
spark.conf.set("spark.hadoop.fs.gs.outputstream.buffer.size", "8388608")  # 8MB
spark.conf.set("spark.hadoop.fs.gs.outputstream.pipe.buffer.size", "1048576")  # 1MB

# Upload configuration
spark.conf.set("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "67108864")  # 64MB
spark.conf.set("spark.hadoop.fs.gs.outputstream.direct.upload.enabled", "true")
```

### Parallelism

```python
# Increase parallelism for large datasets
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## Security Best Practices

### 1. Use Service Account Best Practices

```python
# Use minimal permissions
# Grant only required roles: Storage Object Viewer/Creator

# Rotate keys regularly
# Use short-lived tokens when possible
```

### 2. Enable Bucket Security

```bash
# Enable uniform bucket-level access
gsutil uniformbucketlevelaccess set on gs://my-bucket/

# Enable versioning
gsutil versioning set on gs://my-bucket/

# Set lifecycle rules
gsutil lifecycle set lifecycle.json gs://my-bucket/
```

### 3. Encryption

```python
# Customer-managed encryption keys (CMEK)
spark.conf.set("spark.hadoop.fs.gs.encryption.key.name",
               "projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY")

# Client-side encryption
# Encrypt sensitive data before writing to GCS
```

### 4. Audit Logging

```python
# Enable Cloud Audit Logs for GCS
# Monitor access patterns and detect anomalies
```

## Cost Optimization

### Storage Classes

```python
# Use appropriate storage classes
# Standard: Frequently accessed data
# Nearline: Accessed once per month
# Coldline: Accessed once per quarter
# Archive: Yearly access

# Set storage class during write
spark.conf.set("spark.hadoop.fs.gs.outputstream.storage.class", "NEARLINE")
```

### Lifecycle Management

```json
// lifecycle.json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "SetStorageClass", "storageClass": "NEARLINE" },
        "condition": { "age": 30 }
      },
      {
        "action": { "type": "SetStorageClass", "storageClass": "COLDLINE" },
        "condition": { "age": 90 }
      },
      {
        "action": { "type": "Delete" },
        "condition": { "age": 365 }
      }
    ]
  }
}
```

### Regional Considerations

```python
# Use regional buckets for better performance and lower costs
# Co-locate compute and storage in the same region

# Multi-region only when necessary
# Consider data transfer costs
```

## Troubleshooting

### Authentication Issues

```python
# Debug authentication
import os
print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")

# Test access
try:
    spark.read.text("gs://your-bucket/test.txt").show()
except Exception as e:
    print(f"Error: {e}")

# Verify credentials
os.system("gcloud auth application-default print-access-token")
```

### Permission Errors

```bash
# Check IAM permissions
gcloud projects get-iam-policy PROJECT_ID --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:YOUR_SERVICE_ACCOUNT"

# Grant necessary roles
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT" \
  --role="roles/storage.objectViewer"
```

### Bucket Not Found

```bash
# List accessible buckets
gsutil ls

# Check bucket location
gsutil ls -L -b gs://your-bucket/ | grep Location

# Verify bucket exists
gsutil ls gs://your-bucket/
```

### Performance Issues

```python
# Enable metrics
spark.conf.set("spark.hadoop.fs.gs.performance.cache.enable", "true")

# Monitor GCS metrics in Cloud Console
# Check for:
# - High latency
# - Throttling
# - Network issues

# Optimize based on access patterns
```

### Network Connectivity

```bash
# Test connectivity to GCS
curl -I https://storage.googleapis.com

# Check for proxy settings
echo $HTTP_PROXY
echo $HTTPS_PROXY

# Configure proxy if needed
export HTTPS_PROXY=http://proxy.company.com:8080
```

## Best Practices

1. **Optimize File Layout**

   - Target 100-200 MB file sizes
   - Use consistent naming conventions
   - Avoid deep nesting (impacts list performance)

2. **Partition Strategically**

   - Partition by commonly filtered columns
   - Balance between too many and too few partitions
   - Consider query patterns when designing partitions

3. **Use Appropriate Formats**

   - Parquet for analytics (columnar, compressed)
   - ORC as alternative columnar format
   - JSON for semi-structured data
   - CSV for compatibility (but inefficient)

4. **Implement Data Governance**

   - Use consistent schemas
   - Document data lineage
   - Implement data quality checks
   - Version control your data pipelines

5. **Monitor and Alert**
   - Set up Cloud Monitoring alerts
   - Track storage costs
   - Monitor access patterns
   - Alert on failures and anomalies
