---
title: Google Cloud Storage
rank: 6
---

# Google Cloud Storage

Sail supports reading from and writing to Google Cloud Storage (GCS) buckets.

## URI Format

Google Cloud Storage uses the `gs://` URI scheme.

## Examples

### DataFrame

```python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.parquet("gs://my-bucket/path")

users_df = spark.read.parquet("gs://my-bucket/path")
users_df.show()
```

### SQL

```python
sql = """
CREATE TABLE gcs_table
USING parquet
LOCATION 'gs://my-bucket/path';
"""
spark.sql(sql)
spark.sql("SELECT * FROM gcs_table;").show()

spark.sql("INSERT INTO gcs_table VALUES (3, 'Charlie'), (4, 'Alice');")
spark.sql("SELECT * FROM gcs_table;").show()
```
