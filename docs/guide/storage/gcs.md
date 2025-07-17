---
title: Google Cloud Storage
rank: 6
---

# Google Cloud Storage

Sail supports reading from and writing to Google Cloud Storage (GCS) buckets.

## URL Format

GCS uses the `gs://` URL scheme:

```
gs://bucket-name/path/to/object
```

The URL structure consists of:

- `gs://` - The GCS protocol scheme
- `bucket-name` - The name of your GCS bucket
- `/path/to/object` - The path to the object within the bucket

## Authentication

GCS authentication can be configured through several methods:

### 1. Application Default Credentials

The recommended approach is to use Application Default Credentials:

```bash
# Authenticate using gcloud
gcloud auth application-default login

# Or set the path to a service account key file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### 2. Service Account Key

You can also configure authentication using environment variables:

```bash
export GOOGLE_SERVICE_ACCOUNT="/path/to/service-account-key.json"
# Or embed the JSON directly
export GOOGLE_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'
```

### 3. Workload Identity

When running on Google Cloud Platform (GCP), Sail can automatically use the attached service account through Workload Identity.

## Examples

### Reading Data

```python
# Read a Parquet file from GCS
df = spark.read.parquet("gs://my-bucket/data/file.parquet")

# Read multiple files using wildcards
df = spark.read.parquet("gs://my-bucket/data/*.parquet")

# Read CSV with options
df = spark.read.option("header", "true").csv("gs://my-bucket/data/file.csv")
```

### Writing Data

```python
# Write DataFrame as Parquet
df.write.mode("overwrite").parquet("gs://my-bucket/output/data.parquet")

# Write partitioned data
df.write.partitionBy("year", "month").parquet("gs://my-bucket/partitioned/")

# Write as JSON
df.write.json("gs://my-bucket/output/data.json")
```

### SQL Operations

```sql
-- Create an external table backed by GCS
CREATE TABLE IF NOT EXISTS gcs_table
USING parquet
LOCATION 'gs://my-bucket/data/';

-- Query the table
SELECT * FROM gcs_table WHERE year = 2024;
```

## Performance Considerations

- **Multipart uploads**: Large files are automatically uploaded using multipart uploads for better performance
- **Parallelism**: Sail can read and write multiple files in parallel to improve throughput
- **Caching**: Frequently accessed data can be cached locally to reduce GCS API calls

## Bucket Organization

::: info
GCS uses a flat namespace internally, but supports hierarchical paths using `/` as a delimiter. This means "directories" in GCS are virtual and are created implicitly when objects are uploaded.
:::

Best practices for organizing data in GCS:

- Use consistent naming conventions
- Leverage partitioning for large datasets
- Consider using separate buckets for different environments (dev, staging, prod)
